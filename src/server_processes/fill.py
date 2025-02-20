from . import *
import uuid

from .fill_ext import fill_espdmp, fill_esprho, fill_idsurf, fill_part, fill_espcmp


def kill_woker(session,worker_id):
    # Kill worker
    worker = session.get(Worker, uuid.UUID(worker_id))
    if worker is None:
        raise HTTPException(statudmp_s_code=HTTPStatus.PRECONDITION_FAILED, detail="Worker does not exist")
    session.delete(worker)
    session.commit()

def wrapper_gen_fill(entry, session, worker_id, property):
    try:
        UNIQUE_NAME=get_unique_tag(property)
        kill_woker(session=session, worker_id=worker_id)
        if UNIQUE_NAME==NAME_PART:
            old_record, new_record =fill_part(session,entry)
            return update_record(session, old_record, new_record)
        elif UNIQUE_NAME==NAME_WFN:
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
            old_record=prev_record
            new_record=record
        elif UNIQUE_NAME==NAME_IDSURF:
            old_record, new_record =fill_idsurf(session,entry)
        elif NAME_ESPRHO==UNIQUE_NAME:
            old_record, new_record =fill_esprho(session,entry)
        elif NAME_ESPDMP==UNIQUE_NAME:
            old_record, new_record =fill_espdmp(session, entry)
        elif NAME_ESPCMP == UNIQUE_NAME:
            old_record, new_record = fill_espcmp(session, entry)
        else:
            raise Exception(f"Unkown property \'{UNIQUE_NAME}\'")
        return update_record(session, old_record, new_record)
    except Exception as ex:
        raise Exception(f"Could not execute {wrapper_gen_fill}: {analyse_exception(ex)}")



def extend_app(app, SessionDep):
    """ Http requests to be added to app """

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