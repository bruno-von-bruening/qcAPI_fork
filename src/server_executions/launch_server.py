import datetime, copy
import yaml
import uuid
from typing import Annotated
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Request,Depends
from sqlmodel import SQLModel, func, col, select,delete, Session,create_engine,update
from sqlalchemy.orm import load_only
from fastapi.encoders import jsonable_encoder
from http import HTTPStatus

from data_base.qcAPI_database import (
    #Status,
    Worker,
    RecordStatus,
)
# from data_base.database_declaration import (
#     Compound,
#     Conformation,
#     Wave_Function,
#     Hirshfeld_Partitioning,
#     ISA_Weights,
#     Distributed_Multipoles,
#     #get_conformation_id,
#     #QCRecord,
#     #get_record_id,
#     #RecordStatus,
#     #hirshfeld_partitioning,
#     #Distributed_Multipoles,
# )

from server_processes.populate import populate_functions
from server_processes.get import get_functions
from server_processes.fill import  extend_app
from server_processes.operations import operation_functions
from server_processes.info import info_functions
from server_processes.sending_files import file_functions

def make_app(app, SessionDep):
    """ Add all the methods to the app """
    get=get_functions(app, SessionDep)

    populate=populate_functions(app, SessionDep)

    extend_app(app, SessionDep)

    operation_functions(app, SessionDep)

    info_functions(app, SessionDep)

    app=file_functions(app, SessionDep)



    def get_progress_info(session, property, method, delay):
        """ """
        try:
            filters={}
            if property in ['part']:
                object=hirshfeld_partitioning
                if not isinstance(method, type(None)):
                    filters.update({'method':method})
            elif property in ['wfn']:
                object=QCRecord
            else:
                raise Exception(f"Unkown property {property}")
            
            def get_num(object, filters):
                # Count amount of objget
                query=select(func.count()).select_from(object)
                # Optional filters
                for key,val in filters.items():
                    query=query.where( getattr(object, key) == val )
                # get the number
                query=query
                num=session.exec(query).one()
                return num
            num_records = get_num(object, dict([*filters  ]) )
            converged   = get_num(object, dict([*filters, ('converged',1)   ]) )
            pending     = get_num(object, dict([*filters, ('converged',-1)  ]) )
            # Failed is implicit
            failed = num_records - converged - pending

            # Get numbers of workers that are active (have lasted communicated with database)
            num_workers = session.exec(select(func.count()).select_from(Worker)).one()
            current_timestamp = datetime.datetime.now().timestamp()
            num_active_workers = session.exec(
                select(func.count())
                .select_from(Worker)
                .where(Worker.timestamp > current_timestamp - delay)
            ).one()

            return {
                "message": "qcAPI is running",
                "converged": converged,
                "pending": pending,
                "failed": failed,
                "num_workers": num_workers,
                "recently_active_workers": num_active_workers,
            }
        except Exception as ex:
            detail=f"Error in function {get_progress_info}: {str(ex)}"
            print(detail)
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=detail)

#     @app.get("/{property}")
#     async def root(
#         session: SessionDep, 
#         property: str,
#         delay: float = 600.
#     ):
#         """ Give overview over the job progress
#         Currently one available  """
#         return get_progress_info(session,property, method=None, delay=delay)
# 
#     # --> make this to a generic routine that takes the job class (filter between QCRECORDS and PARITIONINGS)
#     @app.get("/")
#     async def root(session: SessionDep, delay: float = 600.):
#         """ Give overview over the job progress
#         Currently one available  """
#         property='wfn'
#         return get_progress_info(session,property, method=None, delay=delay)
# 
#     @app.post("/conformation_id/")
#     async def return_conformation_id(conformation: Conformation):
#         return get_conformation_id(conformation)
# 

    # Should be update database
    def gen_fill():
        pass
    app=extend_app(app, SessionDep)

    # @app.put("/reset_all_status/")
    # async def reset_all_status(session: SessionDep):
    #     session.exec(update(QCRecord).values(converged=-1))
    #     session.commit()
    #     return {"message": "All records reset to pending status"}

    # @app.put("/reset_failed/")
    # async def reset_failed(session: SessionDep):
    #     session.exec(update(QCRecord).where(QCRecord.converged == 0).values(converged=-1))
    #     session.commit()
    #     return {"message": "Failed records reset to pending status"}

    # #@app.get("/pull/{property}"):
    # #async def get_record(id: str, session: SessionDep):
    # #    record = session.get(QCRecord, id)
    # #    return record



    # # remove record id
    # @app.delete("/delete_record/{id}")
    # async def delete_record(id: str, session: SessionDep):
    #     record = session.get(QCRecord, id)
    #     session.delete(record)
    #     session.commit()
    #     return {"message": "Record deleted successfully"}


    # # list all records ids
    # @app.get("/list_record_ids/")
    # async def list_record_ids(
    #     session: SessionDep,
    #     method: str = None,
    #     basis: str = None,
    #     status: RecordStatus = None,
    # ):
    #     statement = select(QCRecord).options(load_only(QCRecord.id))
    #     if status is not None:
    #         statement = statement.where(QCRecord.converged == status)
    #     if method is not None:
    #         statement = statement.where(QCRecord.method == method)
    #     if basis is not None:
    #         statement = statement.where(QCRecord.basis == basis)
    #     records = session.exec(statement).all()
    #     ids = [record.id for record in records]
    #     return ids

    # # list all records ids
    # @app.get("/list_records/")
    # async def list_records(
    #     session: SessionDep,
    #     method: str = None,
    #     basis: str = None,
    #     status: RecordStatus = None,
    # ):
    #     statement = select(QCRecord)
    #     if status is not None:
    #         statement = statement.where(QCRecord.converged == status)
    #     if method is not None:
    #         statement = statement.where(QCRecord.method == method)
    #     if basis is not None:
    #         statement = statement.where(QCRecord.basis == basis)
    #     records = session.exec(statement).all()
    #     return records

    # # get next record

    #     
    return app



def make_favicon(app):
    """Item to be displayed in browser as item when accessing the server
    generate with favicon generator
    """

    
    import os
    favicon_path=f"{os.path.dirname(__file__)}/favicon.ico"
    from fastapi.responses import FileResponse
    if os.path.isfile(favicon_path):
        @app.get('/favicon.ico', include_in_schema=False)
        async def favicon():
            return FileResponse(favicon_path)
    else:
        print(f"Could not find favicon under {favicon_path}")
    return app

def main(config_file, host, port):
    """ Starts the server """

    # Process the config file
    def process_config_file(config_file):
        # Load config
        with open(config_file) as f:
            config = yaml.safe_load(f)
        db_key='database'
        mand_keys=[db_key]
        for key in mand_keys:
            assert key in config.keys(), f"Expected key \'{key}\' to occur in {config_file}"

        sqlite_file_name = config[db_key].replace('.db','') + ".db"
        return sqlite_file_name
    sqlite_file_name=process_config_file(config_file)

    # Start a session
    def start_engine(sqlite_file_name):
        sqlite_url = f"sqlite:///{sqlite_file_name}"
        connect_args = {"check_same_thread": False}
        engine = create_engine(sqlite_url, connect_args=connect_args, echo=False)
        return engine
    def get_session():
        with Session(engine) as session:
            yield session
    engine=start_engine(sqlite_file_name)
    SessionDep = Annotated[Session, Depends(get_session)]

    # lifespan window (will run at begining and end of server's life) https://fastapi.tiangolo.com/advanced/events/
    def create_db_and_tables():
        SQLModel.metadata.create_all(engine)
    def delete_all_workers():
        with Session(engine) as session:
            # session.query(Worker).delete()
            session.exec(delete(Worker))
            session.commit()
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        create_db_and_tables()
        delete_all_workers()
        yield

    ### LAUNCH the server via uvicorn
    # Make the app
    import uvicorn
    app = FastAPI(lifespan=lifespan)
    app=make_app(app, SessionDep)
    app=make_favicon(app)
    

    uvicorn.run(app,port=port, host=host)