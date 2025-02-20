import sys, os
sys.path.insert(1,os.path.realpath('..'))
from util.util import atomic_charge_to_atom_type, BOHR, ANGSTROM_TO_BOHR, print_flush, make_jobname, make_dir,load_global_config
from util.execution import run_command_in_shell

from pydantic import ValidationError, validate_call
import subprocess as sp
import json, shutil, glob, yaml
import time

from util.util import make_jobname, make_dir
import numpy as np
from data_base.qcAPI_database import RecordStatus
from pydantic import BaseModel, BeforeValidator, PlainSerializer
from typing import List, Annotated, Union, Tuple

from property_database.data_models import File_Model, Map_Stats_Model

from data_base.database_declaration import DMP_ESP_MAP_Stats

from util.run_utils import Tracker
from util.environment import file, directory, link_file
from util.environment import run_shell_command, temporary_file