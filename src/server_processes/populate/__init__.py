from pydantic import BaseModel, validate_call; validate_call=validate_call(config=dict(arbitray_types_allowed=True))
from typing import List
import time

from fastapi import HTTPException
from sqlmodel import select
from http import HTTPStatus
from util.util import (
    get_unique_tag,analyse_exception, my_exception, 
    NAME_CONF,NAME_ESPCMP,NAME_ESPCMP_FILE,NAME_ESPDMP,NAME_ESPRHO,NAME_IDSURF,NAME_PART,NAME_WFN
)
from util.sql_util import create_record, get_primary_key, get_primary_key_name
import numpy as np

from data_base.database_declaration import (
    Wave_Function,ISA_Weights,IsoDens_Surf_File,IsoDens_Surface,Hirshfeld_Partitioning, RHO_ESP_Map, DMP_ESP_Map, DMP_vs_RHO_ESP_Map, Compound, Conformation,
)

from util.trackers import message_tracker, track_ids