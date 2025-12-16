import datetime, copy
import yaml, sys
import uuid
from typing import Annotated
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Request,Depends
from sqlmodel import SQLModel, func, col, select,delete, Session,create_engine,update
from sqlalchemy.orm import load_only
from fastapi.encoders import jsonable_encoder
from http import HTTPStatus

from orm_import.qcAPI_database import (
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

from server_internal.populate.populate import populate_functions
from server_internal.get.get import get_functions
from server_internal.fill.fill import  add_upload_functions
from server_internal.operations import operation_functions
from server_internal.get.info import info_functions
from server_internal.other.restart import RestartRequest, add_other_functions 

from util.config import load_server_config, qcAPI_server_config, qcAPI_storage_info
import os

DEFAULT_CONFIG_FILE="auto_config.yaml"
def make_auto_config_file(host:str|None, port:int|None):
    store=qcAPI_storage_info(
        storage_root_directory=f"{os.getcwd()}/storage",
    )
    def find_setup():
        supposed_path=os.path.join( os.environ['QCPAPI_HOME'], 'install','env_setup.yaml' )
        if os.path.isfile(supposed_path):
            imports=supposed_path
        else:
            warn(f"Could not find setup under default path {supposed_path}")
            imports='find_me'
        return imports
    config=qcAPI_server_config.construct(
        database_file="<make_me>",
        storage_info=store,
        host=host,
        port=port,
        imports=find_setup(),
        
    )
    with open(DEFAULT_CONFIG_FILE, 'w') as f:
        yaml.safe_dump(config.model_dump(), f)
    return DEFAULT_CONFIG_FILE

def make_app_functions(app, SessionDep, storage_info):
    """ Add all the methods to the app """
    get=get_functions(app, SessionDep)

    populate=populate_functions(app, SessionDep)

    add_upload_functions(app, SessionDep, storage_info)

    operation_functions(app, SessionDep)

    info_functions(app, SessionDep)

    add_other_functions(app, SessionDep)


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
    from sqlalchemy import event
    @event.listens_for(engine, "connect")
    def enable_fk(dbapi_conn, conn_record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON;")
    SessionDep = Annotated[Session, Depends(get_session)]
    ### LAUNCH the server via uvicorn
    # Make the app
    app = FastAPI(lifespan=lifespan)
    app=make_app_functions(app, SessionDep, storage_info)
    app=make_favicon(app)
    return app
    

def main(config_file):
    """ Starts the server """

    config=load_server_config(config_file)
    sqlite_file_name=config.database_file
    storage_info=config.storage_info

    def start_server():

        import uvicorn
        import threading
        thread = threading.Thread(target=uvicorn.run, args=(app,), kwargs={"port": config.port, "host": config.host})
        thread.start()
        try:
            while not self.started:
                time.sleep(0.001)
            yield
        finally:
            uvicorn.should_exit = True
            thread.join()

    def restart_server():
        # POSSIBLE: track how many restarts have been done and avoid infinite loops
        print("Restarting server... (will be replace current process if succesful)")
        try:
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as ex:
            raise Exception(f"Error restarting server: {ex}")

    from server_internal.other.restart import restart_flag
    try:
        import uvicorn
        from uvicorn import Config
        import contextlib
        import threading
        import time
        import uvicorn

        from fastapi import FastAPI
        from typing import Generator

        app = app_setup(db_file=sqlite_file_name, storage_info=storage_info)
        conf = Config(app=app, host=config.host, port=config.port, log_level="info")

        class Server(uvicorn.Server):
            @contextlib.contextmanager
            def run_in_thread(self) -> Generator:
                import threading, time, errno

                self._run_exception = None   # store exception from self.run()
                self._bind_error   = None    # flag for address-in-use

                def _runner():
                    self.run()

                thread = threading.Thread(target=_runner)
                thread.start()

                import signal
                def kill_server():
                    os.kill(os.getpid(), signal.SIGTERM)

                time.sleep(0.1)  # give server time to start and possibly fail
                while True:
                    if not self.started:
                        thread.join()
                        break

                    if self.should_exit:
                        thread.join()
                        restart_server()
                        break

                    time.sleep(0.01)

        import server_internal.other.restart as restart_mod
        server = Server(config=conf)
        restart_mod.server = server

        server.run_in_thread()
    except Exception as ex:
        raise Exception(f"Error starting server: {ex}")
