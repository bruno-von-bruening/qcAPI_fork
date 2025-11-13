from . import *

from data_base.database_declaration import (
    Conformation,
    Compound,
    Wave_Function,Wave_Function_Run_Data,
    Hirshfeld_Partitioning,
    MOM_File, Distributed_Multipoles, Distributed_Polarisabilities, Pairwise_Polarisabilities_File,
    ISA_Weights,
    FCHK_File,
    IsoDens_Surface,
    IsoDens_Surf_File,
    DMP_ESP_Map, RHO_ESP_Map, DMP_vs_RHO_ESP_Map,
    RHO_MAP_File, DMP_MAP_File, DMP_vs_RHO_MAP_File,
    DMP_vs_RHO_MAP_Stats, RHO_ESP_MAP_Stats, DMP_ESP_MAP_Stats,
    Group, Group_to_Group, Compound_to_Group, 
)
from util.util import NAME_CONF, NAME_IDSURF, NAME_WFN, NAME_PART, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, NAME_GROUP, NAME_WFN_DAT, NAME_DISPOL
import sys
def make_object_mapper():
    the_mod='data_base.database_declaration'
    keys=[x for x in  dir(sys.modules[the_mod]) if not x.startswith('_') ] 
    return dict( (k,getattr(sys.modules[the_mod], k))  for k in keys)

object_mapper=make_object_mapper()
object_mapper.update({
    NAME_PART: Hirshfeld_Partitioning,
    NAME_WFN: Wave_Function,
    NAME_IDSURF: IsoDens_Surface,
    NAME_ESPDMP: DMP_ESP_Map,
    NAME_ESPRHO: RHO_ESP_Map,
    NAME_ESPCMP: DMP_vs_RHO_ESP_Map,
    NAME_CONF   : Conformation,
    NAME_GROUP  : Group,
    NAME_WFN_DAT : Wave_Function_Run_Data,
    NAME_DISPOL: Distributed_Polarisabilities,
})


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
        mapper=update(mapper, Distributed_Polarisabilities)
        mapper=update(mapper, Pairwise_Polarisabilities_File)
        mapper=update(mapper, Wave_Function_Run_Data)
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


            

