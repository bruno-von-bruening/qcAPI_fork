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

from server_processes.populate.populate import populate_functions
from server_processes.get.get import get_functions
from server_processes.fill import  add_upload_functions
from server_processes.operations import operation_functions
from server_processes.info import info_functions
#from server_processes.sending_files import file_functions

from util.config import load_server_config

def make_app_functions(app, SessionDep, storage_info):
    """ Add all the methods to the app """
    get=get_functions(app, SessionDep)

    populate=populate_functions(app, SessionDep)

    add_upload_functions(app, SessionDep, storage_info)

    operation_functions(app, SessionDep)

    info_functions(app, SessionDep)


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

def app_setup(db_file, storage_info):
    # Start a session
    def start_engine(db_file):
        sqlite_url = f"sqlite:///{db_file}"
        connect_args = {"check_same_thread": False}
        engine = create_engine(sqlite_url, connect_args=connect_args, echo=False)
        return engine
    def get_session():
        with Session(engine) as session:
            yield session

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

    # SQL session
    engine=start_engine(db_file)
    SessionDep = Annotated[Session, Depends(get_session)]
    ### LAUNCH the server via uvicorn
    # Make the app
    app = FastAPI(lifespan=lifespan)
    app=make_app_functions(app, SessionDep, storage_info)
    app=make_favicon(app)
    return app
    

def main(config_file, host, port):
    """ Starts the server """

    config=load_server_config(config_file)
    sqlite_file_name=config.database_file
    storage_info=config.storage_info

    app=app_setup(db_file=sqlite_file_name, storage_info=storage_info)

    import uvicorn
    uvicorn.run(app,port=port, host=host)
