from util.util import get_unique_tag, NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN
from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR

import numpy as np, yaml
import time
import sys,os
from os.path import isfile, isdir
from functools import partial
import subprocess as sp
import json

import requests
from http import HTTPStatus

import multiprocessing as mp

from util.util import get_unique_tag, NAME_CONF, NAME_IDSURF, NAME_PART, NAME_WFN, NAME_ESPRHO, NAME_ESPDMP, NAME_ESPCMP, NAME_GROUP

from pydantic import validate_call
from util.environment import file

from property_database.data_models.utilities import File_Model

from typing import List

from util.config import  load_global_config, get_env, query_config

from data_base.database_declaration import Conformation, Compound