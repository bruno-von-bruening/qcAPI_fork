from . import *

from .get_ext import  create_new_worker, get_next_record

def get_functions(app, SessionDep):

    @app.get("/get/{object}")
    async def get_object(
        object: str,
        session: SessionDep,
    ):
        """ Get a list of all objects of the provided type. """
        try:
            # Assert that object can be assigned to a table
            try:
                object=get_unique_tag(object)
                the_object=object_mapper[object]
            except Exception as ex:
                message=f"Could not match \'{object}\' to a table: {ex}"
                raise Exception(message)

            # Get all tables of the given type
            try:
                results= filter_db(session, object=the_object, filter_args={})
                return {'message':'all good' ,'json':[r.model_dump() for r in results]}
            except Exception as ex:
                message=f"Could not get the entries for property {the_object.__name__}: {ex}"
                raise Exception(message)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, str(ex))

    
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
