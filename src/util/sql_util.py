from . import *
from .util import analyse_exception, my_exception

from sqlmodel import select, Session, SQLModel
#from sqlmodel.orm.session import Session as Session_type
from sqlmodel.main import SQLModelMetaclass as sqlmodel_cl_meta

#my_model= Annotated[SQLModel, BeforeValidator(check_sqlmodel)]

def sqlmodel_formatter(input):
    if isinstance(input, dict):
        return input
    elif issubclass(type(input), BaseModel):
        return input.model_dump()

pdtc_sql_row=Annotated[dict, BeforeValidator(sqlmodel_formatter)]
from sqlmodel.main import SQLModelMetaclass



def filter_db_query(object, filter_args: my_dict):
    """ Generate sqlmodel query"""
    try:
        #assert len(filter_args)>0, f"Did not provide arguments to filter for"
        query=( select(object) )
        for key,val in filter_args.items():
            query=(query
                .filter( getattr(object,key) == val )
            )
        return query
    except Exception as ex:
        raise Exception(f"Could not filter db (object={object.__name__}, filter_args={filter_args}): {analyse_exception(ex)}")

@my_val
def filter_db(session, object, filter_args: my_dict={}):
    """Filter database for given arguments"""
    try:
        query=filter_db_query(object, filter_args=filter_args)
        results=session.exec(query).all()
        return results
    except Exception as ex:
        raise Exception(f"Could not query database: {analyse_exception(ex)}")

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
    return prev_record

def update_record(session, prev_record, record):
    record.timestamp = datetime.datetime.now().timestamp()
    prev_record.sqlmodel_update(record)
    session.add(prev_record)
    session.commit()
    return {"message": "Record stored successfully. Thanks for your contribution!" ,'error':None}

@my_val
def create_record(session, object:SQLModelMetaclass, data: List[pdtc_sql_row|dict]|pdtc_sql_row|dict, commit=True):
    try:
        if not isinstance(data, list):
            data=[ data ]

        def my_add(data):
            instance=object(**data)
            session.add(instance)
            return instance
        instances=[my_add(d) for d in data]
        if commit:
            session.commit()
        
        return instances
    except Exception as ex: my_exception(f"Problem in creating record for object: {object.__name__}")

# Get linker table
def get_primary_key_name(obj):
    from sqlalchemy.inspection import inspect
    primary_key=inspect(obj).primary_key
    assert len(primary_key)==1, f"Object {obj.__name__} has multiple primary keys"
    primary_key=primary_key[0]

    return primary_key.name
def get_primary_key(the_object):
    """  """
    return getattr(the_object, get_primary_key_name(the_object))