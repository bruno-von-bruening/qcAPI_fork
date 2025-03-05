# basic imports
import numpy as np
import json, sys, os
import datetime

# HTTP imports
from fastapi import HTTPException, Request
from http import HTTPStatus


# Database imports
from sqlmodel import select, Session, func, SQLModel
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
)
from data_base.qcAPI_database import (
    Worker, RecordStatus
)

from util.util import analyse_exception, available_properties as AVAILABLE_PROPERTIES
from util import my_dict
from util.sql_util import create_record, update_record, get_prev_record

from util.sql_util import get_next_record_from_db, filter_db, sqlmodel, sqlmodel_cl_meta
from util.util import NAME_CONF, NAME_IDSURF, NAME_WFN, NAME_PART, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, get_unique_tag
object_mapper={
    NAME_PART: Hirshfeld_Partitioning,
    NAME_WFN: Wave_Function,
    NAME_IDSURF: IsoDens_Surface,
    NAME_ESPDMP: DMP_ESP_Map,
    NAME_ESPRHO: RHO_ESP_Map,
    NAME_ESPCMP: DMP_vs_RHO_ESP_Map,
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

        # Values may be double prevent that
        mapper=dict([  (k,list(set([vv.lower() for vv in v]))) for k,v in mapper.items()])

        found=[]
        for the_object, tags in mapper.items():
            if tag.lower() in tags:
                found.append(the_object)
        assert len(found)==1, f"Did not found exately one object for tag \'{tag}\': {found}"

        return classes[found[0]]
    except Exception as ex:
        raise Exception(f"Problem in {get_object_for_tag}: {analyse_exception(ex)}")
    
def get_link(obj, tag:str):
    linked_obj=get_object_for_tag(tag)
    raise Exception(dir(obj), obj._sa_registry, obj._sa_class_manager)
    for key,field_info in obj.model_fields.items():
        
        if hasattr(field_info, "back_populates") and isinstance(field_info.back_populates, str):
            raise Exception(field_info)



from itertools import chain


# Types import
from pydantic import validate_call, ConfigDict, BaseModel
from typing import List, Tuple, Annotated
import uuid
import time
import requests
