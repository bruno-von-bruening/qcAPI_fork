import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")
import numpy as np
import sys, os, copy, json, yaml
from pydantic import ValidationError, validate_call, field_validator
my_val=validate_call(config=dict(arbitrary_types_allowed=True))
val_call=validate_call(config=dict(arbitrary_types_allowed=True))
from pydantic import BaseModel, Field, BeforeValidator, PlainSerializer
from typing import Annotated, Union
import time


import shutil
import datetime
import subprocess as sp

from typing import List, Tuple, Callable

from sqlmodel import Session as session_meta


def check_dict(input):
    if input is None:
        return {}
    elif isinstance(input, dict):
        return input
    else:
        raise ValidationError(f"Provided value is neither None nor dictionary")
my_dict=Annotated[ dict , BeforeValidator(check_dict)]
