from qcp_global_utils.pydantic.pydantic import file, directory

from pydantic import (
    validate_call,
    ValidationError,
    BaseModel, BeforeValidator, PlainSerializer
)
    
val_call=validate_call(config=dict(arbitrary_types_allowed=True))

import sys, os, re, yaml, json, glob, shutil
from functools import partial
import time, datetime

import numpy as np

from typing import List, Annotated, Union, Tuple
from qcp_global_utils.pydantic.pydantic import file, directory
from qcp_global_utils.pydantic.pydantic import file as pdtc_file, directory as pdtc_directory


from util.environment import run_shell_command, temporary_file, compress_file
from sqlmodel.main import SQLModelMetaclass as sqlmodel_cl_meta
from sqlalchemy.sql.schema import Table as sqlalchemy_cl_meta
sqlmodel_meta= Union[sqlmodel_cl_meta|sqlalchemy_cl_meta]
