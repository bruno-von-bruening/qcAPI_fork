from . import *
from util.import_helper import *
from util.sql_util import get_primary_key_name
from orm_import.utils import get_object_for_tag

from sqlmodel import SQLModel


class my_run_data(BaseModel):
    """ Data we want/need to work with"""
    run_directory : pdtc_directory # Directory where the calculation has been ran
    # to_store: Union[ List[pdtc_file] , pdtc_file, None ]= None
    run_files_to_store:     Dict[str, Union[pdtc_file, pdtc_directory]] = {}
                            # Union[ 
                            #     List[Union[pdtc_file|pdtc_directory]] , 
                            #     Union[pdtc_file|pdtc_directory ] 
                            # ] |None=None
    files: Dict[str,pdtc_file] = {}
class job_results(BaseModel):
    run_info: dict={} # not essential
    run_data: my_run_data
    record: SQLModel
    sub_entries: Dict[str,dict|List[dict]|SQLModel|List[SQLModel]]={}
    files_for_entries: Dict[str, Union[pdtc_file, SQLModel]]={}
    def inherit_id_to_subentries(self):
        if not self.sub_entries is None:
            for k,v in self.sub_entries.items():
                def update_with_id(v:dict):
                    the_model=get_object_for_tag(k)
                    prim_key=get_primary_key_name(the_model)
                    if not hasattr(v,prim_key):
                        v.update( {prim_key:id})
                    return v
                if isinstance(v,List):
                    v=[ update_with_id(vi) for vi in v ]
                elif isinstance(v,dict):
                    update_with_id(v)
                elif isinstance(v,SQLModel):
                    update_with_id(v.model_dump())
                else:
                    warn(f"Could not update subentry of type {type(v)} with id, expected dict, list or SQLModel")
                self.sub_entries[k]=v