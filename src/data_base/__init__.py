import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")

from sqlmodel import SQLModel
from pydantic import validate_call


