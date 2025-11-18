from . import *
from util.import_helper import *
from util.sql_util import get_primary_key_name
from data_base.utils import get_object_for_tag

from sqlmodel import SQLModel


class my_run_data(BaseModel):
    """ Data we want/need to work with"""
    run_directory : pdtc_directory # Directory where the calculation has been ran
    # to_store: Union[ List[pdtc_file] , pdtc_file, None ]= None
    run_files_to_store:     Union[ 
                                List[Union[pdtc_file|pdtc_directory]] , 
                                Union[pdtc_file|pdtc_directory ] 
                            ] |None=None
    files: Dict[str,pdtc_file]={}
class job_results(BaseModel):
    run_info: dict={} # not essential
    run_data: my_run_data
    record: SQLModel
    sub_entries: Dict[str,dict|SQLModel]={}
    files_for_entries: Dict[str,pdtc_file]={}
    def inherit_id_to_subentries(self):
        if not self.sub_entries is None:
            for k,v in self.sub_entries.items():
                the_model=get_object_for_tag(k)
                prim_key=get_primary_key_name(the_model)
                if not hasattr(v,prim_key):
                    v.update( {prim_key:id})