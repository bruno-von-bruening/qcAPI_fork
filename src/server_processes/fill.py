from . import *
import uuid


def extend_app(app, SessionDep):
    def kill_woker(session,worker_id):
        # Kill worker
        worker = session.get(Worker, uuid.UUID(worker_id))
        if worker is None:
            raise HTTPException(status_code=400, detail="Worker does not exist")
        session.delete(worker)
        session.commit()

    def wrapper_gen_fill(entry, session, worker_id, property):
        try:
            kill_woker(session=session, worker_id=worker_id)
            if property in ['part']:

                # Check that all is there
                mand_keys=['part','multipoles','solution']
                for key in mand_keys:
                    if not key in entry.keys(): raise Exception(f"Excepted key \'{key}\' but found {entry.keys()}")
                part=entry['part']
                multipoles=entry['multipoles']
                solution=entry['solution']
            
                # Update the partitioning
                try:
                    if part['converged'] < 0:
                        return {"message": "Partitioning not processed. Ignoring."}

                    id=part['id']
                    prev_part = session.get(Hirshfeld_Partitioning, id)
                    if prev_part is None:
                        raise HTTPException(status_code=400, detail="Record does not exist")
                    elif prev_part.converged == 1:
                        raise HTTPException(status_code=210, detail="Record already converged")
                    else:
                        session.delete(prev_part)
                    
                        part=Hirshfeld_Partitioning(**part)
                        part.timestamp = datetime.datetime.now().timestamp()
                        session.add(part)
                        session.commit()
                except Exception as ex:
                    raise HTTPException(501, detail=f"there {part} {ex}")

                # Update the multipoles 
                if not isinstance(multipoles, type(None)):
                    try:
                        multipoles.update({'id':part.id})
                        mul=Distributed_Multipoles(**multipoles)
                        session.add(mul)
                        session.commit()
                    except Exception as ex:
                        raise Exception(f"Could not create multipoles: {analyse_exception(ex)}")
                else:
                    if part.converged==1:
                        raise Exception(f"No multipoles provided although converged!")
                
                # Update the soltuions
                if not isinstance(solution, type(None)):
                    try:
                        solution.update({'id':part.id})
                        sol=ISA_Weights(**solution)
                        session.add(sol)
                        session.commit()
                    except Exception as ex:
                        raise Exception(f"Could not create ISA_weights: {analyse_exception(ex)}")
                else:
                    if part.converged==1:
                        raise Exception(f"No multipoles provided although converged!")

                return {"message": "Partitioning and Moments stored successfully. Thanks for your contribution!", 'error':None}
            elif property in ['wfn']:
                object=Wave_Function
                # id = get_record_id(Conformation(**record.conformation), record.method, record.basis)
                # if id != record.id:
                #     raise HTTPException(status_code=400, detail="ID does not match record")
                id=entry['id']
                prev_record = session.get(object, id)
                if prev_record is None:
                    raise HTTPException(status_code=HTTPStatus.CONFLICT, detail="Record does not exist")
                if prev_record.converged == 1:
                    raise HTTPException(status_code=HTTPStatus.NO_CONTENT, detail="Record already converged")
                
                if entry['converged'] < 0:
                    return {"message": "Record not processed. Ignoring."}
                else:
                    # Create the fchk_entry
                    def create_fchk_entry(id):
                        fchk_key='fchk_info'
                        fchk_info=entry[fchk_key]
                        del entry[fchk_key]
                        assert isinstance(fchk_info, dict), f"Expected dictionary for fchk_info"
                        fchk_obj=FCHK_File(id=id,**fchk_info)

                        session.add(fchk_obj)
                        session.commit()
                        return fchk_obj
                    fchk_obj=create_fchk_entry(id)

                record=object(**entry)


                record.timestamp = datetime.datetime.now().timestamp()
                prev_record.sqlmodel_update(record)
                session.add(prev_record)
                session.commit()
                return {"message": "Record stored successfully. Thanks for your contribution!" ,'error':None}
            else:
                raise Exception(f"Unkown property {property}")
        except Exception as ex:

            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Could not execute {wrapper_gen_fill}: {analyse_exception(ex)}")

    @app.put("/fill/{property}/{worker_id}")
    async def fill(
        entry: dict, 
        worker_id: str, 
        property: str, 
        session: SessionDep, request: Request,
    ):
        try:
            return wrapper_gen_fill(entry, session, worker_id, property)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed request: {str(ex)}")
    
    return app