from . import *

def get_functions(app, SessionDep):
    
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
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failure in execution: {str(ex)}")

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

            def filter(object, status=-1, prop_args={}):
                # Get the next record
                query=( select(object)
                    .filter(object.converged == status)
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
                return record
            record=filter(object, status=-1, prop_args=prop_args)
            #   # in case no record was found start new threads for unfinished records (in case other workers are more powerful or a job is frozen)
            #   if isinstance(record, type(None)):
            #       record=filter(object, status=-2, prop_args=prop_args)

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
                    #   record.converged = -2 # Set this record to running (So it does not get executed doubly)
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
            if property in ['wfn','','wave_function', None]:
                object=Wave_Function
                record, worker_id =get_property(object)
            elif property in ['part','partitioning']:
                # Get result
                object=Hirshfeld_Partitioning
                prop_args={'method':method}
                record, worker_id=get_property(object, prop_args=prop_args)
            else:
                raise Exception(f"Cannot process property \'{property}\'")
        except Exception as ex:
            raise HTTPException(401, detail=f"Error in retrieving record and worker id: {str(ex)}")
        
        return record, worker_id
    @app.get("/get_next/{property}")
    async def get_next(
        session: SessionDep, 
        request: Request,
        property: str,
        method: str = None
    ):
        try:
            record, worker_id=create_new_worker(session,request,property, method)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Could not exectue: {str(ex)}")
        if isinstance(record, type(None)):
            raise HTTPException(HTTPStatus.NO_CONTENT, detail=f"No more record to process!")
        else:
            return record, worker_id
    
    return app