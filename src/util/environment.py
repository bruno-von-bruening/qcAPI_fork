from util.import_helper import *

from qcp_global_utils.pydantic.pydantic import file, directory
from qcp_global_utils.environment.file_handling import temporary_file, link_file, compress_file
from qcp_global_utils.environment.conda_env import get_conda_base, get_python_from_conda_env
from qcp_global_utils.shell_processes.execution import run_shell_command

@val_call
def get_enviornment_variable(variable_name:str, critical:bool=True) -> str:
    if not variable_name in os.environ.keys():
        if critical:
            raise Exception(f"There is no enviornment varaible {variable_name}")
        else:
            return None
    else:
        return os.environ[variable_name]

def get_qcpAPI_home(verbose=True):
    variable_name='QCPAPI_HOME'
    if not variable_name in os.environ.keys():
        if verbose: warn(f"{variable_name} enviornment variable not set, cannot find setup file for server. Will try to continue with default value but might fail if setup file is not there")
        path=None
    else:
        path=os.environ[variable_name]
        if not os.path.isdir(path):
            if verbose: warn(f"{variable_name} enviornment variable is set to {path} but it is not a valid directory. Will try to continue with default value but might fail if setup file is not there")
            path=None
    return path
