from . import *

class counter():
    requested   = 0
    prerequisites_not_met=0
    populated    = 0
    failed = 0



@validate_call
def get_rows(session, sql_table, selection, filter_args:dict={}):
    try:
        # preproces selction (map strings)
        the_selection=[]
        for i,k in enumerate(selection):
            sel=k
            if isinstance(k, str):
                if k=='self':
                    sel = sql_table
                elif k in ['primary_key']:
                    sel = get_primary_key(sql_table)
                else:
                    assert hasattr(sql_table, k), f"Requested to return key \'{k}\' from object {sql_table.__name__} but no such attribute {vars(sql_table)}"
                    sel = getattr(sql_table,k)
            the_selection.append(sel)
        query=select(*the_selection)

        # Filters
        for k,v in filter_args.items():
            assert hasattr(sql_table, k), f"Table \'{sql_table.__name__}\' does not have attribute \'{k}\'"
            query=query.where( getattr(sql_table,k)==v )
        
        # Get and return the ids

        ids= session.exec(query).all() 
        # This returns sqlachemy row objects if more then one is selectd
        if len(the_selection)>1:
            ids=[ list(tuple(x)) for x in ids]
        return ids
    except Exception as ex: my_exception(f"Problem in getting ids:", ex)

@validate_call
def get_ids_for_object(session,sql_table):
    selection=get_primary_key(sql_table)
    return get_rows(session, sql_table, [selection], filter_args={'converged':1})
