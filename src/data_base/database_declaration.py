# import the property database

from . import *

from qcp_database.linked_tables import (
    Compound, 
    Conformation, 
    Group, Group_to_Group, Compound_to_Group,
    #
    Wave_Function, Wave_Function_Run_Data,
    FCHK_File,
    #
    Hirshfeld_Partitioning,  Hirshfeld_Partitioning_Run_Data,
    ISA_Weights, Distributed_Multipoles, MOM_File,
    #
    Distributed_Polarisabilities, Distributed_Polarisabilities_Run_Data,
    Pairwise_Polarisabilities_File,
    #
    IsoDens_Surface,
    IsoDens_Surf_File,
    ### Maps
    # The maps
    RHO_ESP_Map, DMP_ESP_Map, DMP_vs_RHO_ESP_Map,
    # Map files
    RHO_MAP_File, DMP_MAP_File, DMP_vs_RHO_MAP_File,
    # Map statistics
    DMP_vs_RHO_MAP_Stats, DMP_ESP_MAP_Stats, RHO_ESP_MAP_Stats,
    # Polarisabilities
)

#from qcp_database.tables_supplementary import BASE_File
#from sqlmodel import Field, Relationship, SQLModel
#class Wave_Function_Run_Data(BASE_File, SQLModel, table=True):
## Inherit file_model and 
#    class Config:
#        arbitrary_types_allowed=True
#    id: str=Field(foreign_key='wave_function.id', primary_key=True)
#    parent: 'Wave_Function'=Relationship(back_populates='run_data')
#class Wave_Function(Wave_Function):
#    class Config:
#        arbitrary_types_allowed=True
#    run_data: 'Wave_Function_Run_Data'=Relationship(back_populates='parent')
