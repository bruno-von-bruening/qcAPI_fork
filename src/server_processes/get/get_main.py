from . import *
from sqlmodel import SQLModel
from qc_groups.groups_from_qcAPI import generate_tree
from data_base.database_declaration import Group, Group_to_Group, Compound_to_Group



@val_cal
def get_group_structure(session, messanger:message_tracker) -> Union[ message_tracker, List[dict] ]:
    """ Generate the group structure """
    try:
        messanger.start_timing()
        def get_ids(object_table):
            ids=session.exec(
                select( get_primary_key(object_table))
            ).all()
            return ids
        @val_cal
        def my_get(object:SQLModelMetaclass, messanger:message_tracker):
            messanger,obj = get_object(session, messanger, object, get_ids(object))
            return messanger, obj['entries']
        messanger,groups= my_get(Group, messanger)
        messanger,group_to_group= my_get(Group_to_Group, messanger)
        messanger,comp_to_group= my_get(Compound_to_Group, messanger)
    except Exception as ex: my_exception(f"Probelm in getting objects for tree", ex)

    try:
        messanger.start_timing()
        groups=generate_tree(groups, group_to_group, comp_to_group)
        messanger.stop_timing('Generate Tre')
        return messanger, groups
    except Exception as ex: my_exception(f"Problem in generating Tree", ex)


from server_processes.get.get_ext import return_data

@val_cal
def get_object(
    session: session_meta, messanger:message_tracker, object_table: SQLModelMetaclass , ids: List[int|str], links: List[str]=[], filters:dict={}
)->Union[message_tracker,return_data]:
    # Prepare connections links and merge

    try:
        messanger.start_timing()
        return_di=return_data.construct()
        
        the_link_tabs    = [ get_object_for_tag(y) for y in links ]
        for tab in [object_table]+the_link_tabs:
            return_di.primary_keys.update({tab.__name__:get_primary_key_name(tab)})

        # Get id mapper for the linked objects
        tree=get_connections(session,object_table)
        for c in the_link_tabs:
            if not c.__name__ in tree.keys(): raise Exception(f"Could not map object {c.__name__} from object {object_table.__name__}") 
        paths=dict([ (c.__name__,tree[c.__name__]) for c in the_link_tabs])
        mapper=dict([ (name, get_mapper(session,path))  for name, path in paths.items() ])

        messanger.stop_timing('Construction of Links')

    except Exception as ex: my_exception(f"Problem in requested Merged tables:", ex)

    # Make query
    try:
        messanger.start_timing()

        query_seed=filter_db_query(object_table, filters)
        
        query=query_seed.where(get_primary_key(object_table).in_(ids))
        results=session.exec(query).all()
        for i,r in enumerate(results):
            if issubclass(type(r), BaseModel):
                results[i]=r.model_dump()
            elif hasattr(r, "__iter__"):
                results[i]=[ x.model_dump() for x in r]
            else:
                raise Exception(f"Cannot handle type: {type(r)}")
            

        messanger.stop_timing(f"Getting result for main item")
    except Exception as ex: my_exception(f"Problem in making query and executing it:", ex)

    # process results
    try:
        messanger.start_timing()


        if len(links)<1:
            new_results=results
        else:
            new_results=[]
            prim_key=get_primary_key_name(object_table)
            for r in results:
                id=r[prim_key]
                r={object_table.__name__:r}
                for l, the_link in zip(links, the_link_tabs):
                    mapp=mapper[the_link.__name__]                        
                    if not id in mapp.keys(): raise Exception(f"Cannot find key {id} in mapper for {the_link.__name__}")
                    sub_ids=mapp[id]
                    assert isinstance(l, str), f"Expected type of link to be string, got {l}"
                    r.update({l:[ session.get(the_link, id).model_dump() for id in sub_ids]})
                new_results.append(r)

        return_di['entries']=new_results
        
        messanger.stop_timing(f"Finding links")
    except Exception as ex: my_exception(f"Problem in enriching results", ex)

    return messanger, return_di
                