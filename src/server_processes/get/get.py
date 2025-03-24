from . import *

from .get_ext import  create_new_worker, get_next_record, get_objects
from util.sql_util import get_primary_key_name, get_primary_key
from .sending_files_ext import get_file_table, file_response, get_file_from_table

from .util import parse_dict
from ..util.util import object_mapper, get_connections, get_mapper

def get_functions(app, SessionDep):
    @app.get("/get/{object}")
    async def get(
        session     : SessionDep,
        object      : str,
        links       : List[str]=Query([]),
        filters     : List[str]=Query([]),
        ids         : List[str|int]=Query(None),
    ):
        messanger=message_tracker()

        # Checks
        try: 
            messanger.start_timing()

            # Check Filters
            filters=parse_dict(filters)

            # Check table
            object_table=get_object_for_tag(object)

            # Check ids
            if ids is [] or ids is None:
                raise Exception(f"Provided empty ids!")
            elif 'all' in ids:
                ids=session.exec(
                    select( get_primary_key(object_table))
                ).all()
            
            messanger.stop_timing('Preparation')
        except Exception as ex: raise HTTPException(HTTPStatus.BAD_REQUEST, f"Filters argument has error: {analyse_exception(ex)}\nfilters={filters}")


        # Prepare connections links and merge
        try:
            try:
                messanger.start_timing()

                the_link_tabs    = [ get_object_for_tag(y) for y in links ]
                return_di={'entries':{},'primary_keys':{}}
                for tab in [object_table]+the_link_tabs:
                    return_di['primary_keys'].update({tab.__name__:get_primary_key_name(tab)})

                # Get id mapper for the linked objects
                tree=get_connections(object_table)
                paths=dict([ (c.__name__,tree[c.__name__]) for c in the_link_tabs])
                mapper=dict([ (name, get_mapper(session,path))  for name, path in paths.items() ])

                messanger.stop_timing('Construction of Links')

                #   # In case there are multiple links get
                #   if len(the_link_tabs)>1:
                #       subset_order={}
                #       for k,v in paths.items():
                #           subset_order.update({k:[]})
                #           for j,w in paths.items():
                #               if j!=k:
                #                   if set(w).issubset(set(v)):
                #                       subset_order[k].append(j)
                #                   else:
                #                       raise Exception(f"{w} {v}")
                #       def order(dictionary):
                #           # Figure out keys that are below another key that is below the current key
                #           # so to say keys that are doubly below
                #           for upper,Lower_names in dictionary.items():
                #               to_delete=[]
                #               for lower_name in Lower_names:
                #                   Double_lowers=dictionary[lower_name]
                #                   # If a doubly lower is identical to the current lower keys delete thes
                #                   to_delete+=[ double for double in Double_lowers if double in Lower_names]
                #               for del_key in list(set(to_delete)):
                #                   del dictionary[upper][del_key]
                #           return dictionary 
                #       raise Exception( order(subset_order) )

            except Exception as ex: raise my_exception(f"Problem in requested Merged tables:", ex)

            # Make query
            try:
                messanger.start_timing()

                query=select(object_table)
                for k,v in filters.items():
                    assert hasattr(object_table, k), f"{object_table.__name__} does not have attribute {k}: {object_table.__dict__.keys()}"
                    query=query.where(getattr(object_table, k)==v)
                
                query=query.where(get_primary_key(object_table).in_(ids))
                results=session.exec(query).all()
                for i,r in enumerate(results):
                    if issubclass(type(r), BaseModel):
                        results[i]=r.model_dump()
                    elif hasattr(r, "__iter__"):
                        results[i]=[ x.model_dump() for x in r]
                    else:
                        raise Exception(f"Cannot handle type: {type(r)}")
                    

                messanger.stop_timing(f"Getting result for main item")
            except Exception as ex: my_exception(f"Problem in making query and executing it:", ex)
            #the_merge_tabs=[]
            #query=select(object_table, *the_merge_tabs)
            #for m in the_merge_tabs:
            #    query=query.join(m)

            # process results
            try:
                messanger.start_timing()


                if len(links)<1:
                    new_results=results
                else:
                    new_results=[]
                    prim_key=get_primary_key_name(object_table)
                    for r in results:
                        id=r[prim_key]
                        r={object:r}
                        for l, the_link in zip(links, the_link_tabs):
                            mapp=mapper[the_link.__name__]                        
                            sub_ids=mapp[id]
                            r.update({l:[ session.get(the_link, id).model_dump() for id in sub_ids]})
                        new_results.append(r)

                return_di['entries']=new_results
                
                messanger.stop_timing(f"Finding links")
            except Exception as ex: my_exception(f"Problem in enriching results", ex)

            return {**messanger.dump(), 'json':return_di}
                        
                        ## Get the table
                        #query=select( *primary_keys)
                        #for li in the_link_tabs:
                        #    query=query.join(li)

                        #my_linker=[ list(r) for r in session.exec(query).all() ]
                        #return_di['links'][l].update({'linker':my_linker})
                        #return_di['primary_keys'].update({l:get_primary_key_name(the_link)})

                #if len(deps)>0:
                #    dep_di=return_di['dependants']
                #    for id in ids:
                #        dep_di.update({id:{}})
                #        for dep_tab in the_dep_tabs:
                #            # quite inefficient but okay for the moment
                #            childs=[ x[1].model_dump() for x in session.exec(
                #                select(object_table, dep_tab).join(dep_tab).where(get_primary_key(object_table)==id)
                #            )]
                #            dep_di[id].update({dep_tab.__name__:childs})
            
            # #results=get_objects(session, object_table, filters=filters)
            # models=[]
            # for super_row in results:
            #     rel=dict([  (tag,val.model_dump()) for tag, val in zip([object]+links, super_row)  ])
            #     models.append(rel)

        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"{analyse_exception(ex)}")

    @app.get("/get_status/{property}/{id}")
    async def get_status(id: str, property: str, session: SessionDep, worker_id: str = None):
        try:
            if worker_id is not None:
                # update worker timestamp
                worker = session.get(Worker, uuid.UUID(worker_id))
                if worker is not None:
                    worker.timestamp = datetime.datetime.now().timestamp()
                    session.add(worker)
                    session.commit()
            the_object=object_mapper[ get_unique_tag(property) ]
            record = session.get(the_object, id)
                    
            return record.converged
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, analyse_exception(ex))

    @app.get("/get_next/{property}")
    async def get_next(
        session: SessionDep, 
        request: Request,
        property: str,
        method: str = None,
        for_production: bool=True,
    ):
        # Retrieve the record
        try:
            record, worker_id=create_new_worker(session,request,property, method, for_production=for_production)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Could not exectue: {str(ex)}")
        
        # Check if record is empty
        if isinstance(record, type(None)):
            raise HTTPException(HTTPStatus.NO_CONTENT, detail=f"No more record to process!")
        else:
            return record, worker_id
    
    @app.get("/get_file/{file_type}")
    async def get_file( session: SessionDep,
        file_type:str,
        ids: List[str|int]=Query([]),  
    ):
        try:
            file_table=get_file_table(file_type)
        except Exception as ex:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Provided file type argument (\'{file_type}\') could not be recognized: {analyse_exception(ex)}")
        
        try:
            try:
                objects=[session.get(file_table, id) for id in ids]
                files=[ get_file_from_table(x) for x in objects ]
            except Exception as ex: my_exception(f"Problem when recovering files from files:", ex )
            
            try:
                return [ file_response(file) for file in files ][0]
            except Exception as ex: my_exception(f"Error when trying to return files:", ex)
        except Exception as ex: raise HTTPException( HTTPStatus.INTERNAL_SERVER_ERROR, str(ex))
    
    return app
