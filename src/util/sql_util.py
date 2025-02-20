from . import *

from sqlmodel import select, Session, SQLModel
#from sqlmodel.orm.session import Session as Session_type

def filter_db_query(object, filter_args: dict):
    """ Generate sqlmodel query"""
    #assert len(filter_args)>0, f"Did not provide arguments to filter for"
    query=( select(object) )
    for key,val in filter_args.items():
        query=(query
            .filter( getattr(object,key) == val )
        )
    return query

def filter_db(session, object, filter_args: dict={}):
    """Filter database for given arguments"""
    query=filter_db_query(object, filter_args=filter_args)
    results=session.exec(query).all()
    return results

def get_next_record_from_db(session, object, status=-1, prop_args={}):
    """ Returns the pending record with the oldest timestamp given possible filtering arguments """

    # Generate the query
    prop_args.update({'converged':status})
    query=filter_db_query(object, filter_args=prop_args)

    # retrieve oldest record
    query=(query
            .order_by(object.timestamp)
    )
    record=session.exec(query).first()

    return record

def get_prev_record(session, object, id):
    prev_record = session.get(object, id)
    if prev_record is None:
        raise HTTPException(status_code=HTTPStatus.CONFLICT, detail="Record does not exist")
    if prev_record.converged == 1:
        raise HTTPException(status_code=HTTPStatus.NO_CONTENT, detail="Record already converged")
    return prev_record
def update_record(session, prev_record, record):
    record.timestamp = datetime.datetime.now().timestamp()
    prev_record.sqlmodel_update(record)
    session.add(prev_record)
    session.commit()
    return {"message": "Record stored successfully. Thanks for your contribution!" ,'error':None}


def sqlmodel_formatter(input):
    if isinstance(input, dict):
        return input
    else:
        if issubclass(type(input), BaseModel):
            return input.model_dump()
        else:
            raise Exception(f"Type \'{type(input)}\' cannot be understood.")

from sqlmodel.main import SQLModelMetaclass as sqlmodel_cl_meta
sqlmodel=Annotated[dict, BeforeValidator(sqlmodel_formatter)]
@validate_call
def create_record(session, object, data: sqlmodel):
    instance=object(**data)
    session.add(instance)
    session.commit()
    return isinstance