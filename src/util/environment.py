from . import *

from qcp_global_utils.pydantic.pydantic import file, directory
from qcp_global_utils.environment.file_handling import temporary_file, link_file, compress_file
from qcp_global_utils.environment.conda_env import get_conda_base, get_python_from_conda_env
from qcp_global_utils.shell_processes.execution import run_shell_command

@validate_call
def get_enviornment_variable(variable_name:str, critical:bool=True) -> str:
    if not variable_name in os.environ.keys():
        if critical:
            raise Exception(f"There is no enviornment varaible {variable_name}")
        else:
            return None
    else:
        return os.environ[variable_name]
QCAPI_HOME=get_enviornment_variable('QCAPI_HOME')
