from util.import_helper import *
import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")
from server_internal.get.get_ext import return_data
from util.http_util import pdtc_address

from orm_import.utils import  (
    NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN,
    NAME_COMP,
    NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, NAME_GROUP, 
    NAME_DISPOL, NAME_MOLPOL,
    get_object_for_tag,
    get_unique_tag,
)
from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR
from util.util import make_dir

from util.import_helper import *
from util.imports.http_imports import *
from util.imports.mysql_imports import *
from util.util import print_flush

from os.path import isfile, isdir
import subprocess as sp
import requests
from http import HTTPStatus

import multiprocessing as mp
from orm_import.qcAPI_database import RecordStatus





from util.config import  load_global_config, load_worker_config

from orm_import.database_declaration import Conformation, Compound

# PYDANTIC
from typing import List, Union, Tuple

from util.sql_util import sqlmodel_cl_meta, get_primary_key, get_primary_key_name, SQLModelMetaclass

from orm_import.database_declaration import *


from qcpAPI.data_passing import job_results, my_run_data
from orm_import.utils import (
    get_object_for_tag,
)
from util.run_utils import Tracker