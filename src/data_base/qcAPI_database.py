import uuid
from sqlmodel import Field, SQLModel
from enum import Enum

class RecordStatus(int, Enum):
    converged = 1
    failed = 0
    pending = -1
    running = -2

class Worker(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    hostname: str
    timestamp: float = Field(default=-1., index=True)

#class BASE_Status(SQLModel, table=False):
#    id: int=Field(default=None, primary_key=True)
#
#    converged: int = RecordStatus.pending
#    timestamp: float = Field(default=-1., index=True)
#    error: str | None = None
#    message: str | None = None
#    warning: str | None = None
#    elapsed_time: str 
#
#class Status(BASE_Status, SQLModel, table=True):
#    pass