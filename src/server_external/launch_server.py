import datetime, copy
import yaml, sys
import uuid
from typing import Annotated
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Request,Depends
import contextlib # allows to run e.g. with calling server trough a with statment.
from sqlmodel import SQLModel, func, col, select,delete, Session,create_engine,update
from sqlalchemy.orm import load_only
from fastapi.encoders import jsonable_encoder
from http import HTTPStatus

from util.import_helper import *

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

from util.config import load_server_config, qcAPI_server_config, qcAPI_storage_info
import os
from orm_import.database_declaration import Meta

from util.environment import get_qcpAPI_home

DEFAULT_CONFIG_FILE="auto_config.yaml"
def make_auto_config_file(host:str|None, port:int|None, **kwargs):

    def find_setup():
        valid_path=False
        path=get_qcpAPI_home()
        sub_path=('install','env_setup.yaml')
        if not path is None:
            imports=os.path.join( path, *sub_path )
            if not os.path.isfile(imports) and valid_path:
                warn(f"Could not find setup under default path {imports}")
                imports=f"< fix this : {imports} >"
        else:
            imports=os.path.join('<find_me>',*sub_path)
        return imports
    if not 'storage_info' in kwargs:
        kwargs['storage_info']=qcAPI_storage_info(
            storage_root_directory=f"{os.getcwd()}/storage",
        )
    if not 'database_file' in kwargs:
        kwargs['database_file']=f"test_database.db"
    config=qcAPI_server_config.construct(
        host=host,
        port=port,
        **kwargs,
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

    from server_internal.other.restart import add_restart_function
    add_restart_function(app, SessionDep)


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
    def update_meta_table(engine):
        import qcpAPI
        import qcp_orm
        with Session(engine) as session:
            # Check if meta table has entry
            current_meta= Meta(
                    qcp_orm_version=qcp_orm.__version__,
                    qcpAPI_version=qcpAPI.__version__,
            )

            old_meta=session.exec(select(Meta)).all()

            # If not meta exist then make it
            if len(old_meta)==0:
                try:
                    session.merge(current_meta)
                    session.commit()
                except Exception as ex:
                    raise Exception(f"Warning: could not create meta table entry: {ex}") from ex
            # There can only be one!
            elif len(old_meta)>1:
                raise Exception(f"Error: more than one meta table entry found ({len(old_meta)}). There should be only one.")
            # The current meta should agree with the stored one! (otherwise we need complex migrations etc)
            else:
                def compare(meta1,meta2):
                    di1,di2=[ x.model_dump(exclude=['created_on','last_updated','id','other_info']) for x in [meta1,meta2]]
                    from qcp_versioning.helper import get_version_hash
                    disagreements=dict()
                    for key in di1.keys():
                        try:
                            one,two=[ get_version_hash(x[key],key) for x in [di1,di2] ]
                        except Exception as ex:
                            raise Exception(f"Could not get version hash for meta field {key}: {ex}") from ex
                        if one!=two:
                            disagreements[key]=(one, two)
                    return disagreements
                disagreements=compare(old_meta[0], current_meta)
                if len(disagreements)>0:
                    msg="Meta table entry differs from current code version."
                    for key in disagreements.keys():
                        msg += f"\n - field {key}: database has {disagreements[key][0]}, current code has {disagreements[key][1]}"
                    msg += "\n Confirm if you want to continue using this database with the current code version by typing 'y' (otherwise type 'n' to abort): "
                    # Get input y or n
                    user_input = input(msg)
                    if user_input.lower() != 'y':
                        quit("User aborted: not using this database.")
                    else:
                        print("User confirmed: continuing with this database.")


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
        update_meta_table(engine)
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
    

import uvicorn
@contextlib.contextmanager
def start_server(config:pdtc_file|qcAPI_server_config): # -> uvicorn.Server:
    if isinstance(config, str):
        try:
            config=load_server_config(config)
        except Exception as ex:
            raise Exception(f"Could not load config from file {config}: {ex}") from ex
    sqlite_file_name=config.database_file
    storage_info=config.storage_info
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
            def run_in_thread(self):
                import threading, time

                def _runner():
                    self.run()

                thread = threading.Thread(target=_runner, daemon=True)
                thread.start()

                deadline = time.time() + 10
                while not self.started:
                    if not thread.is_alive():
                        raise RuntimeError("Server thread died before becoming ready.")
                    if time.time() > deadline:
                        raise RuntimeError("Timed out waiting for server to start.")
                    time.sleep(0.05)

                return thread

        server = Server(config=conf)

        app.state.server = server
        thread = server.run_in_thread()
        try:
            yield thread
        finally:
            print("Stopping server...")
            server.should_exit = True
            thread.join(timeout=5)

            # If still alive -> force it
            if thread.is_alive():
                print("Force stopping server...")
                server.force_exit = True
                thread.join()
    except Exception as ex:
        raise Exception(f"Error starting server: {ex}")

# @val_call
def main(config:pdtc_file|qcAPI_server_config): # -> uvicorn.Server:
    """ Starts the server """
    import time
    with start_server(config):
        while True:
            time.sleep(1)

    # def start_server():

    #     import threading
    #     thread = threading.Thread(target=uvicorn.run, args=(app,), kwargs={"port": config.port, "host": config.host})
    #     thread.start()
    #     try:
    #         while not self.started:
    #             time.sleep(0.001)
    #         yield
    #     finally:
    #         uvicorn.should_exit = True
    #         thread.join()

    # def restart_server():
    #     # POSSIBLE: track how many restarts have been done and avoid infinite loops
    #     print("Restarting server... (will be replace current process if succesful)")
    #     try:
    #         os.execv(sys.executable, [sys.executable] + sys.argv)
    #     except Exception as ex:
    #         raise Exception(f"Error restarting server: {ex}")

