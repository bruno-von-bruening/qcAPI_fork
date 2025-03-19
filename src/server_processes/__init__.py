# basic imports
import numpy as np
import json, sys, os
import datetime

# HTTP imports
from fastapi import HTTPException, Request, Query
from http import HTTPStatus
from pydantic import validate_call, ValidationError; val_cal=validate_call(config=dict(arbitrary_types_allowed=True))
from typing import Annotated


# Database imports
from sqlmodel import select, Session, func, SQLModel
from data_base.qcAPI_database import (
    Worker, RecordStatus
)

from util.util import analyse_exception, available_properties as AVAILABLE_PROPERTIES
from util.sql_util import create_record, update_record, get_prev_record

from util.sql_util import get_next_record_from_db, filter_db, pdtc_sql_row, SQLModelMetaclass
from util.util import my_exception, get_unique_tag
from typing import Optional
from pydantic import Field, BeforeValidator




@validate_call
def get_prop_pdtc(input):
    if not isinstance(input, str): raise ValidationError
    return get_unique_tag(input)
pdtc_prop=Annotated[str, BeforeValidator(get_prop_pdtc)]
    

from itertools import chain


from util.util import NAME_BSISA, NAME_CONF,NAME_ESPCMP,NAME_ESPCMP_FILE,NAME_ESPDMP,NAME_ESPRHO,NAME_GDMA,NAME_IDSURF,NAME_LISA,NAME_MBIS,NAME_PART,NAME_WFN
from data_base.database_declaration import Wave_Function, ISA_Weights, IsoDens_Surface, Compound, Conformation, Distributed_Multipoles, MOM_File, DMP_vs_RHO_MAP_File,DMP_ESP_Map,DMP_ESP_MAP_Stats,DMP_MAP_File,DMP_vs_RHO_ESP_Map,DMP_vs_RHO_MAP_Stats, FCHK_File, Hirshfeld_Partitioning, IsoDens_Surf_File, RHO_ESP_Map,RHO_ESP_MAP_Stats,RHO_MAP_File
# Types import
from pydantic import validate_call, ConfigDict, BaseModel
from typing import List, Tuple, Annotated
import uuid
import time
import requests
