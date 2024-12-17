#!/usr/bin/env python


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

from database_declaration import (
    Conformation,
    get_conformation_id,
    QCRecord,
    get_record_id,
    RecordStatus,
    Worker,
    hirshfeld_partitioning,
    Molecular_Multipoles,
)

def make_app(app, SessionDep):
    """ Add all the methods to the app """

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
            raise Exception(411, detail=f"Error in function {get_progress_info}: {str(ex)}")

    @app.get("/{property}")
    async def root(
        session: SessionDep, 
        property: str,
        delay: float = 600.
    ):
        """ Give overview over the job progress
        Currently one available  """
        return get_progress_info(session,property, method=None, delay=delay)

    # --> make this to a generic routine that takes the job class (filter between QCRECORDS and PARITIONINGS)
    @app.get("/")
    async def root(session: SessionDep, delay: float = 600.):
        """ Give overview over the job progress
        Currently one available  """
        property='wfn'
        return get_progress_info(session,property, method=None, delay=delay)

    @app.post("/conformation_id/")
    async def return_conformation_id(conformation: Conformation):
        return get_conformation_id(conformation)

    @app.put("/qc_result/{worker_id}")
    async def create_item(
        worker_id: str, record: QCRecord, session: SessionDep, request: Request
    ):
        worker = session.get(Worker, uuid.UUID(worker_id))
        if worker is None:
            raise HTTPException(status_code=400, detail="Worker does not exist")
        session.delete(worker)
        session.commit()

        if record.converged < 0:
            return {"message": "Record not processed. Ignoring."}

        id = get_record_id(Conformation(**record.conformation), record.method, record.basis)
        if id != record.id:
            raise HTTPException(status_code=400, detail="ID does not match record")
        prev_record = session.get(QCRecord, id)
        if prev_record is None:
            raise HTTPException(status_code=400, detail="Record does not exist")
        if prev_record.converged == 1:
            raise HTTPException(status_code=210, detail="Record already converged")

        record.timestamp = datetime.datetime.now().timestamp()
        prev_record.sqlmodel_update(record)
        session.add(prev_record)
        session.commit()
        return {"message": "Record stored successfully. Thanks for your contribution!"}

    # Should be update database
    def gen_fill():
        pass
    def kill_woker(session,worker_id):
        # Kill worker
        worker = session.get(Worker, uuid.UUID(worker_id))
        if worker is None:
            raise HTTPException(status_code=400, detail="Worker does not exist")
        session.delete(worker)
        session.commit()
    def wrapper_gen_fill(entry, session, worker_id, property, method=None):
        try:
            kill_woker(session=session, worker_id=worker_id)
            if property in ['part']:
                mand_keys=['part','multipoles']
                for key in mand_keys:
                    if not key in entry.keys(): raise Exception(f"Excepted key \'{key}\' but found {entry.keys()}")
                part=entry['part']
                multipoles=entry['multipoles']
                try:
                    if part['converged'] < 0:
                        return {"message": "Partitioning not processed. Ignoring."}

                    id=part['id']
                    prev_part = session.get(hirshfeld_partitioning, id)
                    if prev_part is None:
                        raise HTTPException(status_code=400, detail="Record does not exist")
                    elif prev_part.converged == 1:
                        raise HTTPException(status_code=210, detail="Record already converged")
                    else:
                        session.delete(prev_part)
                    
                        part=hirshfeld_partitioning(**part)
                        part.timestamp = datetime.datetime.now().timestamp()
                        session.add(part)
                        session.commit()
                except Exception as ex:
                    raise HTTPException(501, detail=f"there {part} {ex}")


                if not isinstance(multipoles, type(None)):
                    try:
                        mul=Molecular_Multipoles(**multipoles)
                        session.add(mul)
                        session.commit()
                    except Exception as ex:
                        raise HTTPException(502, detail=f"Couldnt create multipoles: {ex}")
                else:
                    if part.converged==1:
                        raise HTTPException(502, detail=f"No multipoles provided although converged!")
                return {"message": "Partitioning and Moments stored successfully. Thanks for your contribution!"}
            elif property in ['wfn']:
                raise Exception(f"Implement {property}")
            else:
                raise Exception(f"Unkown property {property}")
        except Exception as ex:
            raise HTTPException(411, detail=f"Could not execute {wrapper_gen_fill}: {str(ex)}")

    @app.put("/fill/{property}/{method}/{worker_id}")
    async def fill_part(
        entry: dict, 
        worker_id: str, 
        property: str, method: str,
        session: SessionDep, request: Request,
    ):
        return wrapper_gen_fill(entry, session, worker_id, property, method=method)

    def get_conformations(session):
        conformations=[ conf.model_dump() for conf in session.exec(select(QCRecord)).all()  ]
        return conformations
    def update(session,the_class, new_object, ids, force=False):
        """ """
        id=new_object.id
        found=session.get(the_class, id)
        
        if not isinstance(found, type(None)):
            # In case the old entry was not valid, lets try again
            assert 'converged' in found.__dict__.keys(), f"\'converged\' not a key of {the_class} but the existence is assumed in this funtion"
            if force or found.converged != 1:
                found.sqlmodel_update(new_object)
                session.add(found)
                session.commit()
                ids['replaced']+=1
            else:
                ids['omitted']+=1
        else:
            session.add(new_object)
            session.commit()
            ids['newly_inserted']+=1#ids['newly_inserted']+1
        return ids
    def gen_populate(session, property, force=False, method=None):
        ids={
            'omitted':0,
            'replaced':0,
            'newly_inserted':0,
        }
        # Get conformations as dict
        conformations=get_conformations(session)
        for conf in conformations:
            id=conf['id']

            if property in ['part']:
                # Create new object dependant on conformation id!
                if not method.upper() in ['LISA','MBIS']: raise Exception(f"Unkown method: {method.upper()}")
                method=method.upper()
                if not 'fchk_file' in conf.keys(): raise Exception(f"No key \'fchk_file\' in record {conf.keys()}")
                part=hirshfeld_partitioning(record_id=id, method=method, fchk_file=conf['fchk_file'])

                ids=update(session, hirshfeld_partitioning, part, ids, force=force)
            else:
                raise Exception(f"Cannot handle property yet: \'{property}\'")
        
        return ids
    def gen_populate_wrap(property, method, session, force=False):
        """ """
        try:
            if property in ['part']:
                if isinstance(method,type(None)):
                    raise Exception(f"Propertry {property} requires providing a method!")
            ids=gen_populate(session, property, force=force, method=method)
            message='Population Succesful'
            error=None
        except Exception as ex:
            raise HTTPException(215, detail=str(ex))
        return {'ids':ids,'return_message':message, 'advice':f"Have a look into the ids section (omitted would be entries that already have been converged by another job)"}
    @app.post("/populate/{property}")
    async def populate(
        property : str    ,
        session: SessionDep,
        force: bool = False,
    ):
        method=None
        json_return=gen_populate_wrap(property, method, session, force=force)
        return json_return
    @app.post("/populate/{property}/{method}")
    async def populate(
        property : str    ,
        method : str    ,
        session: SessionDep,
        force: bool = False,
    ):
        gen_populate_wrap(property, method, session, force=force)
    @app.post("/populate/wfn/{method}/{basis}")
    async def populate(
        basis: str,
        method: str,
        conformations: List[Conformation],
        session: SessionDep,
        force: bool = False,
    ):
        ids = []
        for conformation in conformations:
            id = get_record_id(conformation, method, basis)
            record = QCRecord(id=id, conformation=jsonable_encoder(conformation), method=method, basis=basis)
            prev_record = session.get(QCRecord, id)
            if prev_record is not None:
                if force or prev_record.converged != 1:
                    prev_record.sqlmodel_update(record)
                    session.add(prev_record)
            else:
                session.add(record)
            ids.append(id)
        session.commit()
        return {"message": "Data inserted successfully", "ids": ids}

    @app.put("/reset_all_status/")
    async def reset_all_status(session: SessionDep):
        session.exec(update(QCRecord).values(converged=-1))
        session.commit()
        return {"message": "All records reset to pending status"}

    @app.put("/reset_failed/")
    async def reset_failed(session: SessionDep):
        session.exec(update(QCRecord).where(QCRecord.converged == 0).values(converged=-1))
        session.commit()
        return {"message": "Failed records reset to pending status"}

    #@app.get("/pull/{property}"):
    #async def get_record(id: str, session: SessionDep):
    #    record = session.get(QCRecord, id)
    #    return record

    @app.get("/get_record/{id}")
    async def get_record(id: str, session: SessionDep):
        record = session.get(QCRecord, id)
        return record
    @app.get("/get_part/{id}")
    async def get_record(id: str, session: SessionDep):
        record = session.get(hirshfeld_partitioning, id)
        return record

    @app.get("/get_record_status/{id}")
    async def get_record_status(id: str, session: SessionDep, worker_id: str = None):
        try:
            record = session.get(QCRecord, id)

            if worker_id is not None:
                # update worker timestamp
                worker = session.get(Worker, uuid.UUID(worker_id))
                if worker is not None:
                    worker.timestamp = datetime.datetime.now().timestamp()
                    session.add(worker)
                    session.commit()
        except Exception as ex:
            raise HTTPException(status_code=202, detail=f"get_record_status did not work but returned {ex}")
                
        return record.converged
    @app.get("/get_part_status/{id}")
    async def get_part_status(id: str, session: SessionDep, worker_id: str = None):
        try:
            record = session.get(hirshfeld_partitioning, id)

            if worker_id is not None:
                # update worker timestamp
                worker = session.get(Worker, uuid.UUID(worker_id))
                if worker is not None:
                    worker.timestamp = datetime.datetime.now().timestamp()
                    session.add(worker)
                    session.commit()
        except Exception as ex:
            raise HTTPException(status_code=202, detail=f"get_part_status did not work but returned {ex}")
                
        return record.converged


    # remove record id
    @app.delete("/delete_record/{id}")
    async def delete_record(id: str, session: SessionDep):
        record = session.get(QCRecord, id)
        session.delete(record)
        session.commit()
        return {"message": "Record deleted successfully"}


    # list all records ids
    @app.get("/list_record_ids/")
    async def list_record_ids(
        session: SessionDep,
        method: str = None,
        basis: str = None,
        status: RecordStatus = None,
    ):
        statement = select(QCRecord).options(load_only(QCRecord.id))
        if status is not None:
            statement = statement.where(QCRecord.converged == status)
        if method is not None:
            statement = statement.where(QCRecord.method == method)
        if basis is not None:
            statement = statement.where(QCRecord.basis == basis)
        records = session.exec(statement).all()
        ids = [record.id for record in records]
        return ids

    # list all records ids
    @app.get("/list_records/")
    async def list_records(
        session: SessionDep,
        method: str = None,
        basis: str = None,
        status: RecordStatus = None,
    ):
        statement = select(QCRecord)
        if status is not None:
            statement = statement.where(QCRecord.converged == status)
        if method is not None:
            statement = statement.where(QCRecord.method == method)
        if basis is not None:
            statement = statement.where(QCRecord.basis == basis)
        records = session.exec(statement).all()
        return records

    # get next record
    def create_new_worker(session, request, property, method=None ):
        def get_property(object, prop_args={}):
            """ Get the next record to be processed
            if no unprocessed records are available break
            else create a worker
            propargs is a dict with key and target value"""
            
            ### GET THE RECORD
            # Check validity of the generic object
            keys=object.__dict__.keys()
            keys=[ k for k in keys if not k.startswith('_')]
            for mandatory_key in ['converged', 'timestamp']+list(prop_args.keys()):
                if mandatory_key not in keys: raise Exception(f"Key {mandatory_key} not in available keys ({keys}) or {object}")
            # Get the next record
            query=( select(object)
                .filter(object.converged == -1)
            )
            # Optional Argument
            for key,val in prop_args.items():
                query=(query
                    .filter( getattr(object,key) == val )
                )
            # Sort and retrieve record
            query=(query
                    .order_by(object.timestamp)
            )
            record=session.exec(query).first()

            #### Decide on continuation either break or create worker
            if not isinstance(record, type(None)):
                # Prepare new record and return it in case this fails send a signal
                try:
                    # Create new worker
                    timestamp = datetime.datetime.now().timestamp()
                    client_host = f"{request.client.host}:{request.client.port}"
                    worker = Worker(hostname=client_host, timestamp=timestamp)
                    session.add(worker)

                    # Update record
                    record.timestamp = timestamp
                    session.add(record)
                    session.commit()
                    session.refresh(record)
                    worker_id=worker.id
                except Exception as ex:
                    raise HTTPException(status_code=499, detail=f"Error in function {ex}")
            else:
                worker_id=None
            return record, worker_id
            
        # Get the record and worker id for the next record
        try:
            if property in ['wfn','', None]:
                object=QCRecord
                record, worker_id =get_property(object)
            elif property in ['part','partitioning']:
                # Get result
                object=hirshfeld_partitioning
                prop_args={'method':method}
                record, worker_id=get_property(object, prop_args=prop_args)
            else:
                raise Exception(f"Cannot process property \'{property}\'")
        except Exception as ex:
            raise HTTPException(300, detail=f"Error in retrieving record and worker id: {str(ex)}")
        
        if isinstance(record, type(None)):
            raise HTTPException(210, detail=f"No more record to process!")
        return record, worker_id

    @app.get("/get_next_record/{property}/{method}")
    async def get_next_record(
        session: SessionDep, 
        request: Request,
        property: str,
        method:  str,
    ):
        record, worker_id=create_new_worker(session,request,property, method)
        return record, worker_id
        
    @app.get("/get_next_record/{property}")
    async def get_next_record_wfn(
        session: SessionDep, 
        request: Request,
        property: str,
    ):
        method=None
        record, worker_id=create_new_worker(session,request,property, method)
        return record, worker_id
    
    return app




def main(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)

    sqlite_file_name = config.get("database_name", "test_database").replace('.db','') + ".db"
    sqlite_url = f"sqlite:///{sqlite_file_name}"

    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, connect_args=connect_args, echo=False)

    def get_session():
        with Session(engine) as session:
            yield session

    def delete_all_workers():
        with Session(engine) as session:
            # session.query(Worker).delete()
            session.exec(delete(Worker))
            session.commit()


    SessionDep = Annotated[Session, Depends(get_session)]


    def create_db_and_tables():
        SQLModel.metadata.create_all(engine)
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        create_db_and_tables()
        delete_all_workers()
        yield

    # Make the app
    import uvicorn
    app = FastAPI(lifespan=lifespan)
    app=make_app(app, SessionDep)
    uvicorn.run(app,port=8000, host='0.0.0.0')

if __name__=='__main__':

    import argparse, os
    description=None
    epilog=None
    par=argparse.ArgumentParser(description=description, epilog=epilog)
    par.add_argument(
        '--config', type=str, help=f"Config file in yaml format", required=True
        )
    args=par.parse_args()
    config_file=args.config
    assert os.path.isfile(config_file), f"Not a file {config_file}"

    main(config_file)


