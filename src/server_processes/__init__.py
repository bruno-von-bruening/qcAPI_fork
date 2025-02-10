from typing import List
from fastapi import HTTPException, Request
from http import HTTPStatus
import numpy as np
import json, sys, os
import datetime

from sqlmodel import select

from util.util import analyse_exception

from data_base.database_declaration import (
    Conformation,
    Compound,
    Wave_Function,
    Hirshfeld_Partitioning,
    Distributed_Multipoles,
    ISA_Weights,
    FCHK_File,
)
from data_base.qcAPI_database import (
    Worker
)