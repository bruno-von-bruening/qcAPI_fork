from util.import_helper import *
import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")

from util.util import get_unique_tag, NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN
from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR


from os.path import isfile, isdir
import subprocess as sp
import requests
from http import HTTPStatus

import multiprocessing as mp
from data_base.qcAPI_database import RecordStatus

from util.util import (
    get_unique_tag,
    NAME_COMP,
    NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, NAME_GROUP, NAME_DISPOL,
)

from qcp_global_utils.shell_processes.execution import run_shell_command

from qcp_database.data_models.utilities import File_Model


from util.config import  load_global_config, load_worker_config

from data_base.database_declaration import Conformation, Compound

# PYDANTIC
from pydantic import validate_call; val_call=validate_call(config=dict(arbitrary_types_allowed=True))
from typing import List, Union, Tuple

from util.sql_util import sqlmodel_cl_meta, get_primary_key, get_primary_key_name

from data_base.database_declaration import *


from server_processes.util.util import get_object_for_tag
