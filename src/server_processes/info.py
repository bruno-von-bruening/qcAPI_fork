from . import *

from .util.util import object_mapper

def info_functions(app, SessionDep):

    # Root function some info when calling the server
    @app.get("/")
    async def root(
        session: SessionDep,
    ):
        return {'message': 'qcAPI server is running'}
        
    @app.get("/info/{object}")
    async def info(
        object : str ,
        session: SessionDep,
    ):
        try:
            timestamp=time.time()
            delay=600

            the_object = get_object_for_tag(object)
            res=filter_db(session, the_object, filter_args={})

            status_mapper=RecordStatus.to_dict()
            id_by_status={}
            for k,v in status_mapper.items():
                id_by_status.update({k: [ x.id for x in res if x.converged==v]})
            
            counts=dict([ (k,len(v)) for k,v in id_by_status.items()])
        
            num_active_workers = session.exec(
                select(func.count())
                .select_from(Worker)
                .where(Worker.timestamp > timestamp - delay)
            ).one()
            counts.update({"recently_active_workers":num_active_workers})
            return counts

        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, analyse_exception(ex))