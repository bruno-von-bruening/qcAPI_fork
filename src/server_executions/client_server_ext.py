from . import *

from run_routines.wave_function.run_psi4_grac import compute_wave_function
from run_routines.part.run_partitioning import exc_partitioning
from run_routines.part.run_partitioning_camcasp import exc_partitioning_camcasp
from run_routines.surfaces.run_isodens_surf import run_isodens_surf
from run_routines.surfaces.run_esp_surf import run_esp_surf
from run_routines.surfaces.run_dmp_esp_surf import run_dmp_esp
from run_routines.surfaces.run_espcmp import run_espcmp
from run_routines.dispol.run_dispol import run_dispol
# sys.path.insert(1, os.environ['SCR'])
# import modules.mod_objects as m_obj
# import modules.mod_utils as m_utl

from util.environment import get_python_from_conda_env

# Associated (satelite) module

def get_camcasp_path():
    CAMCASP='CAMCASP'
    assert CAMCASP in os.environ.keys(), f"Variable \'{CAMCASP}\' is not defined!"
    camcasp_path=os.environ[CAMCASP]
    return camcasp_path

@val_call
def get_python_exc_and_script(config_file:file, tag)->Tuple[file,file]:
    worker_config=load_worker_config(config_file)
    python_env=worker_config.query( ('environment',tag, 'python_env'))
    script_exc=worker_config.query( ('environment',tag, 'script'))
    python_exc=get_python_from_conda_env(python_env)

    return python_exc, script_exc

def prepare_wfn_script(config_file, record, serv_adr, max_iter=None):
    
    def get_geometry(id):
        request_code=f"{serv_adr}/get/Conformation?ids={id}&links=Compound"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get geometry (request={request_code} status_code={status_code}, error={response.text})")
        entries=response.json()['json']['entries']
        assert len(entries)==1, f"Expected one return for id={id} combined with Compound got {len(conf)}"
        links=response.json()['json']['entries'][0]['Compound']
        assert len(links)==1, f"Found not one but {len(links)} Compounds for Conformation: {links}"
        comp=links[0]
        conf = entries[0]['Conformation']
        conf = Conformation(**conf).to_dict()
        return {
            'conf_id':conf['id'],
            'coordinates': np.array(conf['coordinates']),
            'nuclear_charges':conf['elements'].split(),
            'multiplicity': comp['multiplicity'],
            'charge':comp['charge']
        }

    # Get geometry
    conf_id_key='conformation_id'
    assert conf_id_key in record.keys()
    id=record[conf_id_key]
    geom=get_geometry(id)

    tag='psi4'
    python_exc, psi4_script=get_python_exc_and_script(config_file,tag)

    conda_env='psi4'
    script=partial(compute_wave_function, conda_env, psi4_script, geom=geom, max_iter=max_iter)
    return script

def prepare_part_script(config_file, record, serv_adr, max_iter=None):

    def get_wave_function_file(id):
        request_code=f"{serv_adr}/get/FCHK_File?ids={id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get fchk (request={request_code} status_code={status_code}, error={response.text})")
        fchk_info=response.json()['json']['entries']
        assert len(fchk_info)==1, f"Did not found exately one FCHK_file for {id}: {fchk_info}"

        return fchk_info[0]
    
    wfn_id=record['wave_function_id']
    fchk_info=get_wave_function_file(wfn_id)
    #fchk_file=File_Model(**fchk_info).path
    method=record['method']
    if method in ['GDMA','BSISA']:
        camcasp_path= get_camcasp_path()
        python_exc, camcasp_script = get_python_exc_and_script(config_file, 'horton')
        script=partial(exc_partitioning_camcasp, serv_adr, camcasp_path, fchk_info, max_iter=max_iter)

    elif method in ['LISA','MBIS']:
        python_exc, horton_script = get_python_exc_and_script(config_file, 'horton')

        script=partial(exc_partitioning,serv_adr,  python_exc, horton_script, fchk_info, max_iter=max_iter)
    else:
        raise Exception(f"Unknown Method")
    return script

def prepare_idsurf_script(config_file, fchk_file):

    # Get the config for the current file
    idsurf_key='iso_density_surface'
    python_exc, script_exc=get_python_exc_and_script(config_file,idsurf_key)

    script=partial(run_isodens_surf, python_exc, script_exc, fchk_file)
    return script
def prepare_espmap_script(config_file, fchk_file, surface_file):
    """ """
    # Get the config for the current file
    the_key='density_esp'
    the_script=run_esp_surf

    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    mode='fortran'
    if mode=='fortran':
        worker_config=load_worker_config(config_file)
        shell_env_key='shell_env'
        shell_env=worker_config.query( ('environment',the_key, shell_env_key))
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
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    script=partial(the_script, python_exc, script_exc, moment_file, surface_file)
    return script

def prepare_espcmp_script(config_file:file, dmp_map_file:file, rho_map_file:file):
    the_script=run_espcmp
    
    the_key='esp_comparison'
    query=('environment', the_key)
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    script=partial(the_script, python_exc, script_exc, dmp_map_file, rho_map_file)
    return script

#@val_call
def prepare_dispol_script(
    config_file, run_data, address
):
    try:
        fchk_file_id=run_data['fchk_file_id']
        partitioning_entry=run_data['part']
        part_weights=run_data['part_weights'] 
        wfn_entry=run_data['wfn_entry']
    except Exception as ex: raise Exception(ex)

    worker_config=load_worker_config(config_file)

    the_key='camcasp'
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)


    the_script=run_dispol
    script=partial(the_script, python_exc, script_exc, address, wfn_entry, fchk_file_id, partitioning_entry, part_weights)
    return script
