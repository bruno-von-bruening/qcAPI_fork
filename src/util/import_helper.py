from qcp_global_utils.pydantic.pydantic import file, directory, myBaseModel

from typing import (
    Dict, Union, Tuple, Literal, Callable
)
from pydantic import (
    validate_call,
    ValidationError,
    BaseModel, BeforeValidator, PlainSerializer, Field
)

val_call=validate_call(config=dict(arbitrary_types_allowed=True, validate_return=True))

import sys, os, re, yaml, json, glob, shutil
from functools import partial
import time, datetime

import numpy as np

from typing import List, Annotated, Union, Tuple
from qcp_global_utils.pydantic.pydantic import file as pdtc_file, directory as pdtc_directory
from qcp_global_utils.environment.file_handling import load_json_or_yaml


from util.environment import run_shell_command, temporary_file, compress_file
from sqlmodel.main import SQLModelMetaclass as sqlmodel_cl_meta
from sqlmodel.main import SQLModelMetaclass
from sqlmodel import SQLModel
from sqlalchemy.sql.schema import Table as sqlalchemy_cl_meta
sqlmodel_meta= Union[sqlmodel_cl_meta|sqlalchemy_cl_meta]

from qcp_global_utils.shell_processes.execution import run_shell_command
from qcp_global_utils.environment.conda_env import get_python_from_conda_env

from qcp_global_utils.pydantic.pydantic import file as file_pdtc

import logging
def warn(msg):
    msg=f"[WARNING] {msg}"
    print(msg)
    logging.warning(msg)
def info(msg):
    msg=f"[INFO] {msg}"
    print(msg)
    logging.info(msg)