import hashlib
import base64
import uuid
from enum import Enum

import numpy as np
from typing import Optional, List


from pydantic import BaseModel
from sqlmodel import Field, SQLModel, Relationship
from sqlalchemy import Column, PickleType
#from utils import Conformation

from sqlalchemy.orm import registry, with_polymorphic


#       Conformation
#           | 
#   $WFN(method, basis)$_______________
#       |         |__________          |                                         
#  $Partitioning$         $GRID$       |
#    |   |     |            |__________|
#  DMP  SHAPE KLD           |       $ESP-WFN$
#    |    __________________|
#  $ESP-DMP$
#
#   Workers run jobs marked as $

# Helper classes? Files, job_info, WORKER

##### Technical POINTS
#
# Inheritance: Did not manage to do that with SQLModel
# The partitionings may be inhomogenous hence use this as some kind of node
# For potential inheritance (too complicated for the moment) have a look at https://github.com/fastapi/sqlmodel/issues/488
# # mapper_registry = registry()
# # mapper_registry.configure()
# # @mapper_registry.mapped
#   #
#       __mapper_args__ = {
#           "polymorphic_identity": "hirshfeld_partitioning_2",
#       }
#
# For extension of table __table_args__ = {'extend_existing': True}
#
# Links are handled by the database (foreign keys) hence they are implicit and not a "real" part of the class
#

class RecordStatus(int, Enum):
    converged = 1
    failed = 0
    pending = -1

class Worker(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    hostname: str
    timestamp: float = Field(default=-1., index=True)

#class job_data(SQLModel, table=True):
#    id: Optional[int] = Field(default=None, primary_key=True)
#    jobtype
#
#    link to all classes? -> possible one way?

class Conformation(BaseModel):
    species: list[int]
    coordinates: list[list[float]]
    total_charge: int = 0
    energy: float | None = None
    forces: list[list[float]] | None = None
    dipole: list[float] | None = None
    mbis_charges: list[float] | None = None
    mbis_dipoles: list[list[float]] | None = None
    mbis_quadrupoles: list[list[float]] | None = None
    mbis_octupoles: list[list[float]] | None = None
    mbis_volumes: list[float] | None = None
    mbis_volume_ratios: list[float] | None = None
    mbis_valence_widths: list[float] | None = None
    wiberg_lowdin_indices: list[list[float]] | None = None
    mayer_indices: list[list[float]] | None = None
class QCRecord(SQLModel, table=True):

    # This is the root no foreign Keys
    id: str = Field( primary_key=True)

    conformation: Conformation = Field(sa_column=Column(PickleType))
    elapsed_time: float = -1.0
    converged: int = Field(default=-1, index=True)
    method: str = Field(default="none", index=True)
    basis: str = Field(default="none", index=True)
    restricted: bool = Field(default=True, index=True)
    error: str | None = None
    timestamp: float = Field(default=-1., index=True)

    # New keys
    fchk_file: str = Field(default='none')
    
    hirshfeld_partitionings: 'hirshfeld_partitioning' =Relationship(back_populates='record')

#class KLD(SQLModel, table=True):
#    id: int=Field(foreign_key='hirshfeld_partitioning', primary_key=True)
#    KLD: float
#
#    # number_of_electrons: float
#    # KLD is unitless (only depends on density)
#
#    partitioning: 'hirshfeld_partitioning' = Relationship(back_populates='KLD')
class hirshfeld_partitioning(SQLModel, table=True):
    def __init__(self, **kwargs):
        make_id=False
        if not 'id' in kwargs.keys():
            kwargs.update({'id':'TO_BE_REPLACED'})
            make_id=True   
        super().__init__(**kwargs)
        if make_id:
            self.make_id()
    # Identifier
    id: str = Field(primary_key=True)
    record_id: str = Field(foreign_key='qcrecord.id')
    fchk_file: str | None # Copied from record (could be also foreign key)-> no, since this link may break when QCRecord gets updated

    # Specs to method
    method:         str # LISA, MBIS ...
    method_parameters:           str | None = 'DefSet' # basis, settings etc. (will be a mess for the moment)
    # Meta
    timestamp:      float = Field(default=-1, index=True)
    elapsed_time:   float= Field(default=-1.)
    converged:      int = Field(default=-1, index=True)
    error:          str | None = None

    # Links
    molecular_multipoles: 'Molecular_Multipoles' = Relationship(back_populates='partitioning')#,sa_relationship_kwargs={'foreign_keys':[record_id]})
    record:         'QCRecord' = Relationship(back_populates='hirshfeld_partitionings') 

    def make_id(self):
        # Default for method_parameters
        if isinstance(self.method_parameters, type(None)):
            method_parameters='DefSet'
        else:
            method_parameters=self.method_parameters
        part_tag='-'.join([  'PART', self.method, self.method_parameters ])
        id='_'.join([ part_tag, self.record_id])
        self.id=id
        return id
        
    
#    KLD:            'KLD'      = Relationship(back_populates='partitioning')
#    Solution:       'Solution' = Relationship(back_populates='partitioning')
class Molecular_Multipoles(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    partitioning_id: int =Field(foreign_key='hirshfeld_partitioning.id')

    # Object
    multipoles:         str      #json string
    # Meta    
    length_units:       str     = Field(default='BOHR')
    representation:     str     = Field(default='Cartesian')
    convention:         str     = Field(default='Stone')
    traceless:          bool    = Field(default=True)

    # Links
    partitioning: 'hirshfeld_partitioning' = Relationship(back_populates='molecular_multipoles')

def get_conformation_id(conformation: Conformation) -> str:
    coordinates = tuple(np.round(conformation.coordinates, 4).flatten())
    species = tuple(conformation.species)
    total_charge = conformation.total_charge
    h = (coordinates, species, total_charge)

    b64 = base64.urlsafe_b64encode(hashlib.sha3_256(str(h).encode()).digest()).decode("utf-8")

    # the euqal sign is part of the byte encoding to generate unique encoding (see fastapi)
    # The equal sign hinders when reading the file and is hence replaced
    if b64[-1]=='=':
        b64=b64[:-1]+''
    return b64
def get_record_id(conformation: Conformation, method: str, basis:str) -> str:
    b64 = get_conformation_id(conformation)
    return f"{method.lower()}_{basis.lower()}_{b64}"

