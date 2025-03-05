from . import *

from .get_ext import  create_new_worker, get_next_record, get_objects

from fastapi import Query

class dummy(BaseModel):
    object_tag: str
    links: List[str]
    filters: dict={}
class dummy_list(BaseModel):
    the_list: List[dummy]

def get_functions(app, SessionDep):
    @app.get("/get/{object}")
    async def get(
        session: SessionDep,
        object: str,
        links: List[str]=Query([]),
        merge: List[str]=Query([]),
        filters: dict={},
    ):
        
        try:

            return_di={'entries':{},'primary_keys':{}}

            the_object      =get_object_for_tag(object)
            the_links           = [ get_object_for_tag(y) for y in links ]

            
            
            # Get linker table
            def get_prim_key_name(obj):
                from sqlalchemy.inspection import inspect
                primary_key=inspect(obj).primary_key
                assert len(primary_key)==1, f"Object {obj.__name__} has multiple primary keys"
                primary_key=primary_key[0]

                return primary_key.name
            return_di['primary_keys'].update({object:get_prim_key_name(the_object)})
            
            if len(the_links)>0:
                return_di.update({'links':{}})
                for l, the_link in zip(links, the_links):
                    return_di['links'].update({l:{}})

                    def get_prim_key(the_object):
                        return getattr(the_object, get_prim_key_name(the_object))
                    
                    primary_keys=[ get_prim_key(the_object), get_prim_key(the_link) ]
                    primary_key_mapper=dict([  (k,v) for k,v in zip([object,l], [get_prim_key_name(x) for x in [the_object, the_link]])  ])
                    # Get the table
                    query=select( *primary_keys)
                    for li in the_links:
                        query=query.join(li)

                    my_linker=[ list(r) for r in session.exec(query).all() ]
                    return_di['links'][l].update({'linker':my_linker})
                    return_di['primary_keys'].update({l:get_prim_key_name(the_link)})
            

            the_merge=[get_object_for_tag(m) for m in merge]
            query=select(the_object, *the_merge)
            for m in the_merge:
                query=query.join(m)
            #for l in the_links:
            #    query=query.join(l)
            results=session.exec(query).all()
            for i,r in enumerate(results):
                if issubclass(type(r), BaseModel):
                    results[i]=r.model_dump()
                elif hasattr(r, "__iter__"):
                    results[i]=[ x.model_dump() for x in r]
                else:
                    raise Exception(f"Cannot handle type: {type(r)}")
            return_di['entries']=results
            return_di.update({'tables':[object,*merge]})
            
            # #results=get_objects(session, the_object, filters=filters)
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

    
    @app.get("/get/{object}/{id}")
    async def gen_get_object(
        object : str ,
        session: SessionDep,
        id : str | int, 
    ):
        try:
            if object == 'conformation':
                record = session.get(Conformation, id).model_dump()
            elif object == 'geom':
                conf = session.get(Conformation, id)
                comp=conf.compound.to_dict(unpack=True)
                record=dict(
                    coordinates=conf.to_dict(unpack=True)['coordinates'],
                    nuclear_charges=comp['nuclear_charges'],
                    multiplicity=comp['multiplicity'],
                    charge=comp['charge'],
                )
            elif object in ['fchk']:
                wfn=session.get(Wave_Function, id)
                fchk=wfn.wave_function_file.model_dump()
                record=fchk
            else:
                raise Exception(f"Do not know how to handle {object}")
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
