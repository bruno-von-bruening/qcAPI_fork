#!/usr/bin/env python

import os, shutil

# from sqlmodel import  select, SQLModl

from database_declaration import hirshfeld_partitioning, RecordStatus, QCRecord

import database_massages_util as util


failed_value=RecordStatus.failed.value
pending_value=RecordStatus.pending.value
success_value=RecordStatus.converged.value
status_mapper={
    'failed':failed_value,
    'pending':pending_value,
    'successful':success_value,
}

def reset_converged_status(session, object, method=None):

    query_args={}
    if not isinstance(method, type(None)):
        query_args.update({"method":method})

    query_args.update({'converged':failed_value})

    results=util.query(session,object, attribute_targets=query_args)    

    print_info(session, object, method=method)

    # reset the converged status for the found objects
    for r in results:
        r.converged=pending_value
        session.add(r)
        session.commit()
        session.refresh(r)
    print_info(session, object, method=method)

def print_info(session, objects, method=None):
    """ """

    # In case single object provided make it to a list
    if not isinstance(objects, list):
        objects=[objects]
    
    # Methods and list at the same time dont get along!
    if not isinstance(method, type(None)) and len(objects)>1: raise Exception
    
    def get_counts(results):
        counts={}
        for k,v in status_mapper.items():
            the_count=util.count_hits(results, 'converged', v)
            counts.update({k:the_count})
        remainder=len(results)-sum(counts.values())
        if remainder!=0:
            counts.update({"no_interpreation":remainder})
        return counts

    attributes={}
    if not isinstance(method,type(None)):
        attributes.update({'method':method})
    for ob in objects:
        results=util.query(session, ob, attribute_targets=attributes)
        counts=get_counts(results)

        the_string=f"Found for object {ob.__name__}:"
        indent=4*' '
        the_string+=''.join([f"\n{indent}{k:<15} = {v}" for k,v in counts.items()])
        print(the_string)
            







def database_massages(
    db_path, operation,
    method=None, basis=None,
    object_tag=None
):
    def get_object(object_tag):
        info_default=[QCRecord, hirshfeld_partitioning]
        if isinstance(object_tag, type(None)):
            if operation in ['reset']:
                raise Exception()
            elif operation in ['info']:
                object_cl=info_default
            else:
                object_cl=None
        else:
            object_mapper={
                'wfn':QCRecord,
                'part':hirshfeld_partitioning,
            }
            assert object_tag in object_mapper.keys(), f"{object_tag} not in {list(object_mapper.keys())}"
            object_cl=object_mapper[object_tag]
        return object_cl

    # Work on copy of database (for safety)
    tmp_db_path=util.make_temporary_database(db_path)

    # Open a session 
    session=util.open_session(tmp_db_path)

    object_cl=get_object(object_tag)

    func={
        'reset' : reset_converged_status,
        'info'  : print_info,
    }
    args={
        'reset':[object_cl],
        'info':[object_cl],
    }
    kwargs={
        'reset':dict( method=method),
        'info': {},
    }

    the_func, the_args, the_kwargs=(func[operation], args[operation], kwargs[operation] )
    try:
        the_func(session, *the_args, **the_kwargs )
    except Exception as ex:
        raise Exception(f"Error when calling {the_func.__name__}({the_args}, {the_kwargs}):\n    {ex}")

    session.close()

if __name__=="__main__":
    description=""
    epilog=""
    import argparse as ap; par=ap.ArgumentParser(description=description, epilog=epilog)
    # Add argument
    adar=par.add_argument
    operation_choices=['reset','info']
    adar(
        'operation', type=str, help=f"What kind of operation to perform will run a different functionality", choices=operation_choices
    )
    adar(
        '--db', type=str, help=f"File where database is stored",
    )
    object_choices=['wfn','part']
    adar(
        '--object', type=str, help=f"Method of the object", choices=object_choices
    )
    adar(
        '--method', type=str, help=f"Method of the object"
    )
    # parse args
    args=par.parse_args()
    operation=args.operation
    db=args.db
    object_tag=args.object

    # checks
    assert os.path.isfile(db)

    database_massages(db, operation,object_tag=object_tag)
