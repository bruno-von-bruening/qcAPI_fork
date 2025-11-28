from . import *

from .database_declaration import *
from util.auxiliary import my_exception
from util.sql_util import SQLModelMetaclass

@validate_call
def table_mapper(tag:str) -> SQLModel:
    table=[
        DMP_MAP_File,
        RHO_MAP_File,
        DMP_vs_RHO_MAP_File,
        MOM_File,
        FCHK_File,
    ]
    mapper=dict( [(k, [k.__name__])  for k in table])

    found=[]
    for k,avail_tags in mapper.items():
        if tag in avail_tags:
            found.append(k)
    
    assert len(found)==1, f"Could not find table definition associated with tag \'{tag}\'"
    found=found[0]

    return found

# The unique names:
NAME_COMP       ='compound'
NAME_CONF       ='conformation'
NAME_WFN        ='wave_function'
NAME_WFN_FILE   ='FCHK_File'
NAME_MOM_FILE   ='MOM_File'
NAME_PART       ='partitioning'
NAME_IDSURF     ='isodensity_surface'
NAME_ESPRHO     ='density_esp'
NAME_ESPDMP     ='multipolar_esp'
NAME_ESPCMP     ='compare_esp'
NAME_ESPCMP_FILE='espcmp_file'
NAME_GROUP      ='group'
NAME_DISPOL     ='distributed_polarisabilities'
NAME_PAIRPOL_FILE ='pairwise_polarisabilities_file'
NAME_WFN_DAT    ='wave_function_run_data'
NAME_PART_DAT   ='hirshfeld_partitioning_run_data'
NAME_DISPOL_DAT   ='distributed_polarisabilities_run_data'
NAME_MOLPOL     = Molecular_Polarizability.__name__
def make_name_dict():
    # Additional names added to key iteslf
    names={
        NAME_CONF:[],
        NAME_WFN:['wfn'],
        NAME_PART:['part'],
        NAME_IDSURF: ['isosurf'],
        NAME_ESPRHO: ['esprho'],
        NAME_ESPDMP: ['espdmp'],
        NAME_ESPCMP: ['espcmp'],
        NAME_GROUP: [],
        NAME_COMP: [],
        NAME_DISPOL: ['dispol'],
        NAME_PAIRPOL_FILE: [],
        NAME_WFN_FILE: [],
        NAME_MOM_FILE: [],
        NAME_WFN_DAT: [],
        NAME_PART_DAT: [],
        NAME_DISPOL_DAT: [],
        NAME_MOLPOL: [],
    }
    # Names for functions, key will be added to list
    [ names[k].append(k) for k in names.keys()]
    return names
names=make_name_dict()
def make_available_properties(names: dict) -> List[float]:
    avail_prop=[]
    for k,v in names.items():
        avail_prop +=[k]+list(v) 
AVAILABLE_PROPERTIES=make_available_properties(names)
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
                mapper.update({name:[name.lower()]})
                classes.update({name:table})
            if not key is None:
                mapper[name].append(key.lower())
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
        mapper=update(mapper, Molecular_Polarizability)
        mapper=update(mapper, Molecular_Polarizability_Run_Data)
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

@val_call
def get_unique_tag(object:str|SQLModelMetaclass, print_options: bool =False)-> str:
    if isinstance(object, SQLModelMetaclass):
        object=object.__name__
    def do_print_options():
        lines=[f"Following options are accepted:"]
        indent=4*' '
        max_leng=max([ len(the_key) for the_key in names.keys() ])
        for the_key, aliases in names.items():
            aliases_key=','.join(aliases)
            the_key=the_key+' '*(max_leng-len(the_key))
            lines+=[f"{indent}- {the_key} ( aliases={aliases_key} )"]
        return '\n'.join(lines)
        
    # Get a unique name for the object
    object=object.lower()
    found_tags=[]
    for prop, tags in names.items():
        if object in [x.lower() for x in tags]:
            found_tags.append(prop)
    if len(found_tags)!=1:
        if not print_options:
            raise Exception(f"Option {object} cannot be interpreted\n"+do_print_options())
        else:
            quit(f"Option {object} cannot be interpredted\n"+do_print_options())
    else:
        object_tag=found_tags[0]
    return object_tag