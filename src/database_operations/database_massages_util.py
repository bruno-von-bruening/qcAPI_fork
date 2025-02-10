import os, shutil
from sqlmodel import Session, create_engine, SQLModel, select
import numpy as np


def open_session(db_path):
    sqlite_url=f"sqlite:///{db_path}"
    engine=create_engine(sqlite_url)
    return Session(engine)

def make_temporary_database(db_path):

    assert os.path.isfile(db_path)
    assert db_path.lower().endswith('.db')
    
    new_path=os.path.join(
        os.path.dirname(db_path) , 'tmp.'+os.path.basename(db_path)
    )
    shutil.copy(db_path, new_path)

    return new_path

def query(session,object, attribute_targets={}):

    # object needs to be sqlmodel table
    assert issubclass(object, SQLModel)

    query=select(object)
    for k,v in attribute_targets.items():
        query=query.where( getattr(object, k) == v)
    
    results=session.exec(query).all()

    return results

def count_hits(a_list, property, value):
    def get_val(entry, property):
        try:
            the_value=getattr(entry, property)
            return the_value
        except Exception as ex:
            try:
                the_value=entry.__dict__[property]
                return the_value
            except Exception as ex:
                raise Exception(f"Could not find {property} in {entry}")
    the_count=len([ 
                True for x in a_list 
                if  get_val(x, property)==value 
        ])
    return the_count
