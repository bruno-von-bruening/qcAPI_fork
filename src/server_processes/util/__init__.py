
from util.util import my_exception
from pydantic import validate_call; val_cal=validate_call(config=dict(arbitrary_types_allowed=True))
from typing import List, Tuple, Callable
from sqlmodel import SQLModel, select
from sqlalchemy import inspect
from util.sql_util import sqlmodel_cl_meta, get_primary_key