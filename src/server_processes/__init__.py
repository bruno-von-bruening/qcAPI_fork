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
    NAME_ESPCMP: DMP_vs_RHO_ESP_Map
}
from itertools import chain

# Types import
from pydantic import validate_call, ConfigDict, BaseModel
from typing import List, Tuple, Annotated
import uuid
import time
import requests
