from util.import_helper import *
import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")

from orm_import.utils import *
from orm_import.utils import get_unique_tag
from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR

from util.import_helper import *
from util.imports.http_imports import *
from util.imports.mysql_imports import *

from os.path import isfile, isdir
import subprocess as sp
import requests
from http import HTTPStatus

import multiprocessing as mp
from orm_import.qcAPI_database import RecordStatus

from qcp_global_utils.shell_processes.execution import run_shell_command



from util.config import  load_global_config, load_worker_config

from orm_import.database_declaration import Conformation, Compound

# PYDANTIC
from pydantic import validate_call; val_call=validate_call(config=dict(arbitrary_types_allowed=True))
from typing import List, Union, Tuple

from util.sql_util import sqlmodel_cl_meta, get_primary_key, get_primary_key_name

from orm_import.database_declaration import *


from orm_import.utils import get_object_for_tag

from fastapi import HTTPException