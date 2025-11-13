
from . import *
from qcp_global_utils.shell_processes.execution import run_shell_command
from qcp_global_utils.environment.file_handling import temporary_file
    # url='http://localhost:9000/' #get/ISA_Weights\?ids\=LISA_n_pbe0-grac_aVTZ_-0q3cY2OmHWrOcpFEuiBQhXEvYy8t-pJnGkZiUZtM0A\?links\=conformation'
    # import json
    # from qcp_global_utils.shell_processes.execution import run_shell_command

    # try:
    #     run_shell_command(f"wget {url}")# -O {isa_json_file}")
    # except Exception as ex: raise Exception(f"Problem in reaching server: {ex}")


from qcp_database.data_models.isa import ISA_Weights_Base
from qcp_objects.objects.basis import molecular_radial_basis
import json, os
from receiver.get_request import get_file


from qcp_global_utils.shell_processes.execution import run_shell_command
from qcp_global_utils.environment.file_handling import temporary_file
@val_call
def make_isa_weight_file(isa_weight_model:dict) -> temporary_file:
    dump_shape_file='weights.json'

    sol=ISA_Weights_Base(**isa_weight_model)
    dic=sol.to_object().model_dump()
    basis=molecular_radial_basis(**dic)
    dic=basis.model_dump()

    with open(dump_shape_file, 'w') as wr:
        json.dump(dic, wr)
    return dump_shape_file

def get_fchk_file(host_url, fchk_file_id:str) -> temporary_file:
    fchk_file_path=get_file(address=host_url , object='FCHK_File',id=fchk_file_id)
    tmp_file=temporary_file(fchk_file_path)
    return tmp_file

@val_call
def run_camcasp(executable:file, script:file,  fchk_file:file, weight_file:file):
    cmd=f"{executable} {script} --wfn {fchk_file} --part {weight_file} isapol --exc"
    run_shell_command(cmd)