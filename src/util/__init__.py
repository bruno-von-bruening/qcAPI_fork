import numpy as np
import sys, os, copy, json
from pydantic import ValidationError, validate_call
from pydantic import BaseModel, Field, BeforeValidator, PlainSerializer
from typing import Annotated
import time


import shutil
import datetime
import subprocess as sp

from typing import List
from pydantic import validate_call


def check_dict(input):
    if input is None:
        return {}
    elif isinstance(input, dict):
        return input
    else:
        raise ValidationError(f"Provided value is neither None nor dictionary")
my_dict=Annotated[ dict , BeforeValidator(check_dict)]