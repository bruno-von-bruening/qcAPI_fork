from . import *

from .database_declaration import *

@validate_call
def table_mapper(tag:str) -> SQLModel:
    table=[
        DMP_MAP_File,
        RHO_MAP_File,
        DMP_vs_RHO_MAP_File,
        MOM_File,
        FCHK_File,
    ]
    mapper=dict( [(k, [k.__name__])  for k in table])

    found=[]
    for k,avail_tags in mapper.items():
        if tag in avail_tags:
            found.append(k)
    
    assert len(found)==1, f"Could not find table definition associated with tag \'{tag}\'"
    found=found[0]

    return found
