from . import *

from .get_ext import  create_new_worker, get_next_record, get_objects

from fastapi import Query

class dummy(BaseModel):
    object_tag: str
    links: List[str]
    filters: dict={}
class dummy_list(BaseModel):
    the_list: List[dummy]


# Get linker table
def get_prim_key_name(obj):
    from sqlalchemy.inspection import inspect
    primary_key=inspect(obj).primary_key
    assert len(primary_key)==1, f"Object {obj.__name__} has multiple primary keys"
    primary_key=primary_key[0]

    return primary_key.name
def get_prim_key(the_object):
    return getattr(the_object, get_prim_key_name(the_object))

def get_functions(app, SessionDep):
    @app.get("/get/{object}")
    async def get(
        session: SessionDep,
        object: str,
        links: List[str]=Query([]),
        merge: List[str]=Query([]),
        filters: List[str]=Query([]),
        ids: List[str|int]=Query(None),
        deps: List[str]|None=Query([]),
    ):
        if ids is []:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Provided empty ids!")
        elif 'all' in ids:
            get_all=True
        else:
            get_all=False
        
        try:
            new_filters={}
            for x in filters:
                sep='--'
                ar=x.split(sep)
                assert len(ar)==2, f"Expected separator \'{sep}\' in filters option but got {x}"
                new_filters.update({ar[0]:ar[1]})
            filters=new_filters
        except Exception as ex: raise HTTPException(HTTPStatus.BAD_REQUEST, f"Filters argument has error: {analyse_exception(ex)}\nfilters={filters}")
        
        try:
            return_di={'entries':{},'primary_keys':{},'dependants':{},'links':{}}

            # Get the object table
            object_table=get_object_for_tag(object)
            # Get 
            the_link_tabs    = [ get_object_for_tag(y) for y in links ]
            the_dep_tabs     = [ get_object_for_tag(y) for y in deps ]
            the_merge_tabs   = [ get_object_for_tag(y) for y in merge]

            if get_all:
                ids=session.exec(
                    select( get_prim_key(object_table))
                ).all()

            for tab in [object_table]+the_dep_tabs+the_link_tabs:
                return_di['primary_keys'].update({tab.__name__:get_prim_key_name(tab)})
            

            query=select(object_table, *the_merge_tabs)
            for m in the_merge_tabs:
                query=query.join(m)
            for k,v in filters.items():
                assert hasattr(object_table, k), f"{object_table.__name__} does not have attribute {k}: {object_table.__dict__.keys()}"
                query=query.where(getattr(object_table, k)==v)
            query=query.where(get_prim_key(object_table).in_(ids))
            results=session.exec(query).all()

            for i,r in enumerate(results):
                if issubclass(type(r), BaseModel):
                    results[i]=r.model_dump()
                elif hasattr(r, "__iter__"):
                    results[i]=[ x.model_dump() for x in r]
                else:
                    raise Exception(f"Cannot handle type: {type(r)}")
            return_di['entries']=results
            
            if len(links)>0:
                for l, the_link in zip(links, the_link_tabs):
                    return_di['links'].update({l:{}})

                    
                    primary_keys=[ get_prim_key(object_table), get_prim_key(the_link) ]
                    primary_key_mapper=dict([  (k,v) for k,v in zip([object,l], [get_prim_key_name(x) for x in [object_table, the_link]])  ])
                    # Get the table
                    query=select( *primary_keys)
                    for li in the_link_tabs:
                        query=query.join(li)

                    my_linker=[ list(r) for r in session.exec(query).all() ]
                    return_di['links'][l].update({'linker':my_linker})
                    return_di['primary_keys'].update({l:get_prim_key_name(the_link)})

            if len(deps)>0:
                dep_di=return_di['dependants']
                for id in ids:
                    dep_di.update({id:{}})
                    for dep_tab in the_dep_tabs:
                        # quite inefficient but okay for the moment
                        childs=[ x[1].model_dump() for x in session.exec(
                            select(object_table, dep_tab).join(dep_tab).where(get_prim_key(object_table)==id)
                        )]
                        dep_di[id].update({dep_tab.__name__:childs})

            return_di.update({'tables':[object,*merge]})
            
            # #results=get_objects(session, object_table, filters=filters)
            # models=[]
            # for super_row in results:
            #     rel=dict([  (tag,val.model_dump()) for tag, val in zip([object]+links, super_row)  ])
            #     models.append(rel)

            return {'message':'all_good', 'json':return_di}
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"{analyse_exception(ex)}")
                




    #@app.get("/get/{object}")
    #async def get_object(
    #    object: str,
    #    session: SessionDep,
    #):
    #    """ Get a list of all objects of the provided type. """
    #    try:
    #        results= get_objects_for_tag(session,object)
    #        return {'message':'all good' ,'json':[r.model_dump() for r in results]}
    #    except Exception as ex:
    #        raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, str(ex))

    
    # @app.get("/get/{object}/{id}")
    # async def gen_get_object(
    #     object : str ,
    #     session: SessionDep,
    #     id : str | int, 
    # ):
    #     try:
    #         # Find the record in the database
    #         if 'geom' == object:
    #             conf = session.get(Conformation, id)
    #             comp=conf.compound.to_dict(unpack=True)
    #             record=dict(
    #                 coordinates=conf.to_dict(unpack=True)['coordinates'],
    #                 nuclear_charges=comp['nuclear_charges'],
    #                 multiplicity=comp['multiplicity'],
    #                 charge=comp['charge'],
    #             )
    #         elif 'fchk' == object:
    #             wfn=session.get(Wave_Function, id)
    #             fchk=wfn.wave_function_file.model_dump()
    #             record=fchk
    #         else:
    #             raise Exception()

                

            
            return record
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failure in execution: {analyse_exception(ex)}")

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
    
    return app
