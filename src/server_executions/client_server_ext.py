from . import *

from run_routines.run_psi4_grac import compute_wave_function
from run_routines.run_partitioning import exc_partitioning
from run_routines.run_isodens_surf import run_isodens_surf
from run_routines.run_esp_surf import run_esp_surf
from run_routines.run_dmp_esp_surf import run_dmp_esp
from run_routines.run_espcmp import run_espcmp
# sys.path.insert(1, os.environ['SCR'])
# import modules.mod_objects as m_obj
# import modules.mod_utils as m_utl

from util.util import load_global_config, query_config
from util.environment import get_python_from_conda_env

# Associated (satelite) module
from util.util import atomic_charge_to_atom_type, BOHR, ANGSTROM_TO_BOHR

def prepare_wfn_script(config_file, record, serv_adr, max_iter=None):
    
    def get_psi4_script(config_file):
        sys.path.insert(1,os.path.realpath('..'))
        global_config=load_global_config(config_file)
        psi4_script=global_config['psi4_script']
        return psi4_script
    def get_geometry(id):
        request_code=f"{serv_adr}/get/geom/{id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get geometry (request={request_code} status_code={status_code}, error={response.text})")
        geom=response.json()
        geom.update({'conf_id':id})

        return geom

    # Get geometry
    conf_id_key='conformation_id'
    assert conf_id_key in record.keys()
    id=record[conf_id_key]
    geom=get_geometry(id)
    psi4_script=get_psi4_script(config_file)
    script=partial(compute_wave_function, psi4_script, geom=geom, max_iter=max_iter)
    return script

def prepare_part_script(config_file, record, serv_adr, max_iter=None):
    def get_horton_script(config_file):
        sys.path.insert(1,os.path.realpath('..'))
        global_config=load_global_config(config_file)
        horton_key='horton_script'
        assert horton_key in global_config.keys(), f"Cannot find {horton_key} in \'{config_file}\': {global_config}"
        psi4_script=global_config['horton_script']
        return psi4_script

    def get_wave_function_file(id):
        request_code=f"{serv_adr}/get/fchk/{id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get fchk (request={request_code} status_code={status_code}, error={response.text})")
        fchk_info=response.json()

        return fchk_info
    
    def gen_fchk_file(fchk_info):
        target_host=fchk_info['hostname']
        this_host=os.uname()[1]
        assert this_host==target_host, f"File from a different hostname!: here=\'{this_host}\', there=\'{target_host}\'"

        to_storage  =fchk_info['path_to_container']
        to_file     =fchk_info['path_in_container']
        file        =fchk_info['file_name']
        path=os.path.realpath(os.path.join(os.environ['HOME'], to_storage, to_file, file))
        assert os.path.isfile(path), f"Not a existing file: {path}"
        return path
        

    horton_script=get_horton_script(config_file)
    wfn_id=record['wave_function_id']
    fchk_info=get_wave_function_file(wfn_id)
    fchk_file=gen_fchk_file(fchk_info)

    script=partial(exc_partitioning, horton_script, fchk_file, max_iter=max_iter)
    return script

def prepare_idsurf_script(config_file, fchk_file):

    # Get the config for the current file
    idsurf_key='iso_density_surface'
    query=('global', idsurf_key)
    python_env=query_config(config_file, (*query,'python_env') )
    script_exc=query_config(config_file, (*query, 'script' ))
    python_exc=get_python_from_conda_env(python_env)

    script=partial(run_isodens_surf, python_exc, script_exc, fchk_file)
    return script
def prepare_espmap_script(config_file, fchk_file, surface_file):
    """ """
    # Get the config for the current file
    the_key='density_esp'
    the_script=run_esp_surf

    query=('global', the_key)
    python_env=query_config(config_file, (*query,'python_env') )
    script_exc=query_config(config_file, (*query, 'script' ))
    python_exc=get_python_from_conda_env(python_env)

    mode='fortran'
    if mode=='fortran':
        shell_env_key='shell_env'
        shell_env=query_config(config_file, (*query, shell_env_key))
        assert isinstance(shell_env, dict), f"Expected dictionary under {shell_env_key} in {config_file} {[*query]}"
    else:
        shell_env=None

    script=partial(the_script, python_exc, script_exc, fchk_file, surface_file, shell_env=shell_env, mode=mode)
    return script
@validate_call
def prepare_espdmp_script(
    config_file:str, moment_file:str, surface_file:str#, ranks:str
):
    """ """
    the_script=run_dmp_esp
    the_key='multipolar_esp'
    query=('global', the_key)
    python_env=query_config(config_file, (*query,'python_env') )
    script_exc=query_config(config_file, (*query, 'script' ))
    python_exc=get_python_from_conda_env(python_env)

    script=partial(the_script, python_exc, script_exc, moment_file, surface_file)
    return script

def prepare_espcmp_script(config_file:file, dmp_map_file:file, rho_map_file:file):
    the_script=run_espcmp
    
    the_key='esp_comparison'
    query=('global', the_key)
    python_env=query_config(config_file, (*query,'python_env') )
    script_exc=query_config(config_file, (*query, 'script' ))
    python_exc=get_python_from_conda_env(python_env)

    script=partial(the_script, python_exc, script_exc, dmp_map_file, rho_map_file)
    return script