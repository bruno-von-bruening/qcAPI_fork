from . import *
from .util import analyse_exception, my_exception
import re

from sqlmodel import select, Session, SQLModel
from sqlalchemy import inspect
#from sqlmodel.orm.session import Session as Session_type
from sqlmodel.main import SQLModelMetaclass as sqlmodel_cl_meta
from sqlalchemy.sql.schema import Table as sqlalchemy_cl_meta
sqlmodel_cl_meta= Union[sqlmodel_cl_meta|sqlalchemy_cl_meta]

#my_model= Annotated[SQLModel, BeforeValidator(check_sqlmodel)]

def sqlmodel_formatter(input):
    if isinstance(input, dict):
        return input
    elif issubclass(type(input), BaseModel):
        return input.model_dump()

pdtc_sql_row=Annotated[dict, BeforeValidator(sqlmodel_formatter)]
from sqlmodel.main import SQLModelMetaclass

# Get  all ids
@val_call
def  get_ids_for_table(session: session_meta, the_object:SQLModelMetaclass, filtered_ids:str|List[str|int]='all'):
    """ Get ids of a given object and filter for given ids in case ids have been provided """
    # Get available keys
    prim_key=get_primary_key_name(the_object)
    all_objects=session.exec( select(the_object)).all()
    all_ids=[ getattr(x,prim_key) for x in all_objects]
    
    if isinstance(filtered_ids,str):
        if filtered_ids=='all':
            the_ids=all_ids
        else: raise Exception(f"Unkown key for conformations_ids: \'{filtered_ids}\'")
    else:
        id_not_there=[ the_id for the_id in all_ids if the_id not in filtered_ids ]
        the_ids=[ the_id for the_id in all_ids if the_id in filtered_ids ]
        for the_id in id_not_there: id_tracker.add_prequisites_not_met(the_id)
    return the_ids

def filter_db_query(object, filter_args: my_dict):
    """ Generate sqlmodel query
    And already provide filters
    If the keys are integers or varchars then if a list is provided the filter will be interpreted as in command else as equals
    """
    try:
        # The seed of the query
        query=( select(object) )
        
        def get_field(object, key): 
            assert hasattr(object, key), f"{object.__name__} does not have attribute {key}: {object.__dict__.keys()}"
            the_field=getattr(object, key)
            field_type=the_field.type
            return the_field,str(field_type)
        def add_to_query(query, the_field, field_type, value):
            # The filter commands differ for list of single item provided

            try:            
                type_mapper={
                    'INTEGER':int,
                    'VARCHAR':str,
                    'FLOAT':float,
                }
                # replace the size of the varchar
                size_tag=re.search(r'\(.*\)', field_type)
                if not size_tag is None:
                    field_type=field_type.replace( size_tag.group(0), '' ) # remove the size of the varchar
                
                assert field_type in type_mapper.keys(), f"Unkown database type: {field_type}"
                the_type=type_mapper[field_type]

                if not isinstance(value, list):
                    if not isinstance(value, the_type):
                        try:
                            value=the_type(value)
                        except Exception as ex: raise Exception(f"Expected {the_type} but found {value} which cannot be casted")
                    query=(query
                        .filter( the_field == value )
                    )
                else:
                    assert isinstance(value[0], the_type ), f"Excpected data type {str(field_type)} but filters (list of values) gives values as {type(value)}: {value[0]}"
                    query=query.where( the_field.in_(val))
                return query    
            except Exception as ex: my_exception(f"Could not add filter to query",ex)

        for key,val in filter_args.items():
            # Check if field exists for given key and get this field
            field,field_type=get_field(object, key)

            # 
            query=add_to_query(query, field, field_type, val)

        return query
    except Exception as ex:
        raise my_exception(f"Could not filter db (object={object.__name__}, filter_args={filter_args}):", ex)

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

def update_record(session, prev_record, record, commit=True):
    if hasattr(prev_record, 'timestamp'):
        record.timestamp = datetime.datetime.now().timestamp()
    prev_record.sqlmodel_update(record)
    if commit: 
        session.add(prev_record)
        session.commit()
    return prev_record
    #{"message": "Record stored successfully. Thanks for your contribution!" ,'error':None}

@my_val
def create_record(session:Session, object:SQLModelMetaclass, data: List[pdtc_sql_row|dict]|pdtc_sql_row|dict, 
    commit=True, 
    update_if_exists=False,
):
    try:
        def my_add(instance, update_if_exists=False):
            #instance=object(**data)
            try: 
                if update_if_exists:
                    prev_rec=get_prev_record(session, object, getattr(instance,prim_key))
                    if not prev_rec is None:
                        instance=update_record(session, prev_rec, instance, commit=False)
                session.add(instance)
                return instance
            except Exception as ex: my_exception(f"Cannot create record:", ex)
        
        if not isinstance(data, list):
            data=[ data ]
        if update_if_exists:
            prim_key=get_primary_key_name(object)
        
        data=[ object(**d) if isinstance(d, dict) else d for d in data ]
        
        instances=[my_add(d, update_if_exists=update_if_exists) for d in data]
        if commit:
            session.commit()
        
        return instances
    except Exception as ex: my_exception(f"Problem in creating record for object: {object.__name__}", ex)


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

@val_call
def get_links(table:sqlmodel_cl_meta, object_map:dict):
    links=inspect(table).relationships.items()
    table_names=[ str(x[1].target) for x in links]
    tables=[ tag_to_object(x, object_map) for x in table_names ]
    return tables

#@val_cal
#def make_connector(table:sqlmodel_cl_meta):
#    connector={}
#    def update_connector(table, connector):
#        table_names=get_links(table)
#        connector.update({table.__name__:table_names})
#        return connector
#    
#    connector=update_connector(table, connector)
#    i=0
#    while i<100:
#        i+=1
#        leaves=[]
#        for v in connector.values():
#            leaves+=v
#        
#        to_update=[]
#        for l in leaves:
#            if l not in connector.keys():
#                to_update.append(l)
#        if len(to_update)==0:
#            break
#        else:
#            for l in to_update:
#                connector=update_connector(get_object_for_tag(l), connector)
#    return connector


def get_object_to_tag(session):
    engine=session.get_bind()#, reflect=True)
    from sqlalchemy import MetaData
    META_DATA = MetaData()
    META_DATA.create_all(engine)
    META_DATA.reflect(engine)

    tables=list(META_DATA.tables.values())
    table_names=[ str(tab) for tab in tables]

    the_mod='data_base.database_declaration'
    keys=dir(sys.modules[the_mod])
    mapper=[]
    for tab in table_names:
        matched_keys=[key for key in keys if key.lower()==tab]
        if len(matched_keys)==0: pass #aise Exception(f"no object {tab} in {the_mod}: {keys}")
        else:
            found_objects=[ getattr(sys.modules[the_mod], key) for key in matched_keys]
            found=[ obj for obj in found_objects if isinstance(obj, sqlmodel_cl_meta)]
            assert len(found)==1, f"Not exactely one found for \'{tag}\': {found}"
            found=found[0]
        mapper+=[ (tab, found)] 
    return dict(mapper)
        
    #raise Exception(dir(tables[0]))

    #from sqlalchemy.ext.declarative import declarative_base

    #Base = declarative_base(metadata=META_DATA)

    ##return dict(META_DATA.tables)

    #classes, models, table_names = [], [], []
    #from sqlmodel import SQLModel
    #my_registry=Base.registry._class_registry
    #raise Exception(list(Base.tables))
    #for clazz in my_registry.values():
    #    raise Exception(clazz)
    #    try:
    #        table_names.append(clazz.__tablename__)
    #        classes.append(clazz)
    #    except:
    #        pass
    #for table in my_registry.items():
    #    if table[0] in table_names:
    #        models.append(classes[table_names.index(table[0])])
    #raise Exception(models)

def tag_to_object(tag, tag_to_object):
    if not tag.lower() in tag_to_object.keys():
        raise Exception(f"Did not find {tag} in dict {tag_to_object}")
    else:
        return tag_to_object[tag.lower()]
@val_call
def get_paths(table:sqlmodel_cl_meta, object_to_tag_map):
    try:
        @val_call
        def get_childs(table, exclusion: List[str]|None=None):
            try:
                childs=get_links(table, object_to_tag_map)
                #childs = connections[table.__name__]
                #childs  = [get_object_for_tag(c) for c in childs]
                if not exclusion is None:
                    childs=[ c for c in childs if not c.__name__ in exclusion]
                return childs
            except Exception as ex: raise my_exception(f"",ex)

        # The list of paths we want to have
        paths=[]
        # The new paths that we want to follow (add will generate new paths in case they link to unseen objects)
        process_paths=[ ( table.__name__, ) ]
        # List of objects already visists (to prevent circular branches)
        already_linked=[ table.__name__]   

        i=0
        while i<100: 
            i+=1

            added_paths=[]
            if len(process_paths)==0:
                break
            else:
                for x in process_paths:
                    last_member=tag_to_object(x[-1], object_to_tag_map)
                    childs=get_childs(last_member, exclusion=already_linked)

                    new_paths=[ (*x, c.__name__) for c in childs ]
                    added_paths+=new_paths

                    already_linked+=[ c.__name__ for c in childs]
                
                paths+=added_paths
                process_paths=added_paths

        return [ x for x in paths if len(paths)>1 ]
    except Exception as ex: my_exception(f"Problem in generating paths", ex)

@val_call
def get_connections(session: session_meta, table:sqlmodel_cl_meta):
    """ Return dictionary of every object that is linked with this table as keys and the path it can be reached by as value """
    try:
        # All the unique paths
        object_to_tag_map=get_object_to_tag(session)
        paths=get_paths(table,object_to_tag_map)

        # Organize them in a dictinoary with key the target and value the path to the target
        connections={}
        for p in paths:
            assert isinstance(p, (list, tuple)), f"Expected list or tuple got {p}"
            assert len(p)>1
            assert p[0]==table.__name__
            leaf=p[-1]
            
            if leaf in connections.keys(): raise Exception(f"Got second path for leaf {leaf}, that is not intended!")
            else: connections.update({leaf: p})
        return connections

    except Exception as ex: my_exception(f"Problem in Generating connection:", ex)

@val_call
def get_mapper(session,path:List[str]):
    """ Get a dictinoary that links the ids of compounds 
    Takes the path through which objects are connected.
    """

    try:
        id_mappers=[]
        tag_to_object_map=get_object_to_tag(session)
        for i in range(len(path)):
            if i < len(path)-1:
                
                pair=[ tag_to_object(x, tag_to_object_map) for x in path[i:i+2]]
                combinations=session.exec( select(get_primary_key(pair[0]), get_primary_key(pair[1])).join(pair[1]) ).all()
                dic={}
                for k,v in combinations:
                    if not k in dic.keys():
                        dic.update({k:[]})
                    dic[k].append(v)
                id_mappers.append(dic)
        
        if len(id_mappers)==1:
            return id_mappers[0]
        else:
            try:
                total_mapper={}
                total_mapper.update(id_mappers[0])
            except Exception as ex:
                raise Exception(id_mappers[0])
            for dic in id_mappers[1:]:
                for k,v in total_mapper.items():
                    new_vals=[]
                    for x in v:
                        try:
                            new_vals+=dic[x]
                        except Exception as ex: Exception(f"Do not find {x} in keys of mapping dicitonary: {list(dic.keys())[:3]} ...")
                    total_mapper[k]=new_vals
            return total_mapper
    except Exception as ex: my_exception(f"Problem in generating mapper", ex)

@val_call
def get_defining_attributes(table:sqlmodel_cl_meta):
    defining_attributes_key='_defining_keys'
    if not hasattr(table, defining_attributes_key): raise Exception(f"Expected key \'{defining_attributes_key}\' in {table}")
    defining_attributes_keys=getattr(table, defining_attributes_key).default
    defining_attributes=[ getattr(table, x) for x in defining_attributes_keys]
    return defining_attributes_keys, defining_attributes


# Take into account converged and not converged
@val_call
def get_duplicate_entries(
    session:session_meta, table:sqlmodel_cl_meta #, messanger:messanger|None=None
)-> List[int|str]|np.ndarray:
    """ """
    try:
        defining_attributes_keys, defining_attributes=get_defining_attributes(table)

        data=session.exec( select(get_primary_key(table), *defining_attributes)).all()
        
        ids=[ d[0] for d in data]
        attributes=np.array([ d[1:] for d in data])
        #id_type=get_primary_key(table).annotation

        uniques, unique_indices, inverse, counts = np.unique(attributes, axis=0, return_index=True,return_counts=True, return_inverse=True)
        
        # sanity checks for assingment of np unique output
        assert counts.sum()==len(attributes), f"Sum of counds and length of rows is not the same! {counts.sum()}!={len(attributes)}"
        #assert len(inverse[inverse>len(attributs)])==0 
        assert len(unique_indices)==len(uniques), f"{len(unique_indices)}!={len(uniques)}"

        index_groups=[]
        indexed_inverse=np.array([ [i,x] for i,x in enumerate(inverse) ])
        for i in range(len(uniques)):
            match=indexed_inverse[indexed_inverse[:,1]==i]
            index_groups+=[ [ ids[i] for i in match[:,0]] ]

        return index_groups, uniques
    except Exception as ex: my_exception(f"Problem in finding dubplicates", ex)

