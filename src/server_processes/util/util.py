from . import *

from data_base.database_declaration import (
    Conformation,
    Compound,
    Wave_Function,
    Hirshfeld_Partitioning,
    MOM_File, Distributed_Multipoles,
    ISA_Weights,
    FCHK_File,
    IsoDens_Surface,
    IsoDens_Surf_File,
    DMP_ESP_Map, RHO_ESP_Map, DMP_vs_RHO_ESP_Map,
    RHO_MAP_File, DMP_MAP_File, DMP_vs_RHO_MAP_File,
    DMP_vs_RHO_MAP_Stats, RHO_ESP_MAP_Stats, DMP_ESP_MAP_Stats,
    Group, Group_to_Group, Compound_to_Group, 
)
from util.util import NAME_CONF, NAME_IDSURF, NAME_WFN, NAME_PART, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, NAME_GROUP
object_mapper={
    NAME_PART: Hirshfeld_Partitioning,
    NAME_WFN: Wave_Function,
    NAME_IDSURF: IsoDens_Surface,
    NAME_ESPDMP: DMP_ESP_Map,
    NAME_ESPRHO: RHO_ESP_Map,
    NAME_ESPCMP: DMP_vs_RHO_ESP_Map,
    NAME_CONF   : Conformation,
    NAME_GROUP  : Group,
}
def get_object_for_tag(tag):
    try:
        classes=dict([ (v.__name__,v) for k,v in object_mapper.items()])

        mapper=dict([ (v.__name__,[k, v.__name__]) for k,v in object_mapper.items()])

        def update(mapper,table, key=None):
            name=table.__name__
            if not name in mapper.keys():
                mapper.update({name:[name]})
                classes.update({name:table})
            if not key is None:
                mapper[name].append(key)
            return mapper
        
        mapper=update(mapper, Compound, 'compound')
        mapper=update(mapper, Conformation, 'conformation')
        mapper=update(mapper, DMP_vs_RHO_ESP_Map)
        mapper=update(mapper, DMP_vs_RHO_MAP_Stats)
        mapper=update(mapper, DMP_vs_RHO_MAP_File)
        mapper=update(mapper, FCHK_File)
        mapper=update(mapper, Group)
        mapper=update(mapper, ISA_Weights)
        mapper=update(mapper, Distributed_Multipoles)
        mapper=update(mapper, MOM_File)
        mapper=update(mapper, IsoDens_Surf_File)
        for obj in [Group, Group_to_Group, Compound_to_Group]:
            mapper=update(mapper, obj)
        for x in [DMP_ESP_Map,RHO_ESP_Map,DMP_ESP_MAP_Stats,DMP_vs_RHO_MAP_File, DMP_vs_RHO_ESP_Map, DMP_ESP_MAP_Stats, DMP_MAP_File, RHO_MAP_File, RHO_ESP_MAP_Stats]:
            mapper=update(mapper, x)

        # Values may be double prevent that
        mapper=dict([  (k,list(set([vv.lower() for vv in v]))) for k,v in mapper.items()])

        found=[]
        for the_object, tags in mapper.items():
            if tag.lower() in tags:
                found.append(the_object)
        assert len(found)==1, f"Did not found exately one object for tag \'{tag}\': {found}"

        return classes[found[0]]
    except Exception as ex:
        raise my_exception(f"Problem in {get_object_for_tag}:", ex)

@val_cal
def get_links(table:sqlmodel_cl_meta):
    links=inspect(table).relationships.items()
    table_names=[ str(x[1].target) for x in links]
    tables=[ get_object_for_tag(x) for x in table_names ]
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

@val_cal
def get_paths(table:sqlmodel_cl_meta):
    try:
        @val_cal
        def get_childs(table, exclusion: List[str]|None=None):
            childs=get_links(table)
            #childs = connections[table.__name__]
            #childs  = [get_object_for_tag(c) for c in childs]
            if not exclusion is None:
                childs=[ c for c in childs if not c.__name__ in exclusion]
            return childs

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
                    last_member=get_object_for_tag(x[-1])
                    childs=get_childs(last_member, exclusion=already_linked)

                    new_paths=[ (*x, c.__name__) for c in childs ]
                    added_paths+=new_paths

                    already_linked+=[ c.__name__ for c in childs]
                
                paths+=added_paths
                process_paths=added_paths

        return [ x for x in paths if len(paths)>1 ]
    except Exception as ex: my_exception(f"Problem in generating paths", ex)

@val_cal
def get_connections(table:sqlmodel_cl_meta):
    """ Return dictionary of every object that is linked with this table as keys and the path it can be reached by as value """
    try:
        # All the unique paths
        paths=get_paths(table)

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

@val_cal
def get_mapper(session,path:List[str]):

    try:
        id_mappers=[]
        for i in range(len(path)):
            if i < len(path)-1:
                
                pair=[ get_object_for_tag(x) for x in path[i:i+2]]
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

            

