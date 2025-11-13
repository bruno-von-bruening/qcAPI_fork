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
from util.util import print_flush

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

    tag='run_psi4'
    python_exc, psi4_script=get_python_exc_and_script(config_file,tag)

    script=partial(compute_wave_function, python_exc, psi4_script, geom=geom, max_iter=max_iter)
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

def wait_for_job_completion(srv_adr, record, worker_id, res, delay, property):
    """ Check status of job until it changes.
    The job may be done by another worker, then return this info in job_already_done variable"""

    job_already_done = False # Should stay false for regular termination
    entry=None
    while not job_already_done:
        try:
            delay = np.random.uniform(0.8, 1.2) * delay
            entry = res.get(timeout=delay)
            break
        except mp.TimeoutError:
            response = requests.get(f"{srv_adr}/get_status/{property}/{record['id']}?worker_id={worker_id}")
            if response.status_code != HTTPStatus.OK:
                print_flush(
                    f"Error getting record status. Got status code: {response.status_code} , text={response.text}"
                )
                continue
            job_status = response.json()
            print_flush("JOB STATUS: ", job_status)
            job_already_done = ( job_status in [1,0] )
    
    if isinstance(entry, dict):
        
        # Recovering the avaible information
        info_lines=[]
        for key in  ['message','error','warnings']:
            if key in entry.keys():
                info_lines.append(f"{key.capitalize():<10} : {entry[key]}")
        if len(info_lines)==0:
            info_lines=[f"No information tags where found in the output"]
        
        # Formatting and printing
        info_string=f"Job completition:"
        indent=4*' '
        info='\n'.join([info_string]+ [ indent+x for x in info_lines])
        print(info)

    return entry, job_already_done
def get_next_record(serv_adr, property='part', method='lisa', for_production=True):
    """ Get a the next record to be worked at (in case there is none, return none) """
    while True:

        request_code='/'.join([
                'get_next', property ])
        opts=[('method',method), ('for_production',True)]
        opts=[ f"{k}={v}" for k,v in opts if v!=None]
        request_code+=f"?{'&'.join(opts)}"

        the_request=os.path.join(serv_adr, request_code)
        response = requests.get(the_request)
        status_code=response.status_code
        
        # Break because there are no jobs left
        if status_code == HTTPStatus.OK:
            body = response.json()
            record,worker_id = body
            break
        elif status_code == HTTPStatus.NO_CONTENT:
            print_flush("No more records. Exiting.")
            worker_id, record= (None, None)
            break
        elif status_code== HTTPStatus.INTERNAL_SERVER_ERROR:
            raise Exception(f"{HTTPStatus.INTERNAL_SERVER_ERROR} ({request_code}): (received code {status_code}, detail={response.text})")
        elif status_code== HTTPStatus.UNPROCESSABLE_ENTITY:
            raise Exception(f"Invalid request to server: {the_request}: detail={response.text}")
        elif status_code==HTTPStatus.NOT_FOUND:
            raise Exception(f"Server did not find request {request_code}: {status_code} {response.text}")
        else:
            error=f"Unkown Error"
            print(f"{error} ({request_code}): Retrying in a bit. (received code {status_code}, detail={response.text})")
        time.sleep(0.5)
    return record, worker_id

def prepare_script(config_file, serv_adr, record, UNIQUE_NAME, max_iter):
    # Decide which function to use and define arguments
    prod_key='production_data' # Key under which production data is dumped
    assert prod_key in record.keys(), f"Could not find key \'{prod_key}\' in record keys ({list(record.keys())})"
    prod_data=record[prod_key]
    del record[prod_key]
    if UNIQUE_NAME==NAME_WFN:
        script = prepare_wfn_script(config_file, record, serv_adr, max_iter=max_iter)
    elif NAME_PART==UNIQUE_NAME:
        script = prepare_part_script(config_file, record, serv_adr, max_iter=max_iter)
    elif UNIQUE_NAME==NAME_IDSURF:
        fchk_file=prod_data['fchk_file']
        script = prepare_idsurf_script(config_file, fchk_file=fchk_file)
    elif NAME_ESPRHO==UNIQUE_NAME:
        fchk_file=prod_data['fchk_file']
        surface_file=prod_data['surface_file']
        script = prepare_espmap_script(config_file, fchk_file=fchk_file, surface_file=surface_file)
    elif NAME_ESPDMP==UNIQUE_NAME:
        moment_file=prod_data['moment_file']
        surface_file=prod_data['surface_file']
        script = prepare_espdmp_script(config_file, moment_file=moment_file, surface_file=surface_file)
    elif NAME_ESPCMP == UNIQUE_NAME:
        rho_map_file=prod_data['rho_map_file']
        dmp_map_file=prod_data['dmp_map_file']
        script = prepare_espcmp_script(config_file, dmp_map_file=dmp_map_file, rho_map_file=rho_map_file)
    elif NAME_DISPOL == UNIQUE_NAME:
        script=prepare_dispol_script(config_file, prod_data, serv_adr)
    else:
        raise Exception(f"No routine defined for {UNIQUE_NAME}")
    return script
    # After extraction clean the record

def job_wrapper(config_file, serv_adr, UNIQUE_NAME, max_iter, delay, record, worker_id, num_threads, target_dir, do_test):
    args=(record, worker_id)
    kwargs={'do_test':do_test , 'num_threads':num_threads,'target_dir':target_dir}
    script=prepare_script(config_file, serv_adr, record, UNIQUE_NAME, max_iter)
    # Start the job
    pool = mp.Pool(1) # Why is this here
    assert hasattr(script, '__call__'), f"Provide function for execution, got {script}"
    proc = pool.apply_async(script, args=args, kwds=kwargs)
    # Check return of job
    entry, job_already_done =wait_for_job_completion(serv_adr, record, worker_id,proc, delay, UNIQUE_NAME)
    return pool,entry, job_already_done

@val_call
def pack_run_directory(working_directory:directory, run_directory: List[str]|directory, the_model: sqlmodel_cl_meta, id:str|int, worker_id:str) -> pdtc_file:
    
    # Check that the working direcotry is the 
    # This is not really elegant but a check is better than deleting something undesired
    if isinstance(run_directory, str):
        assert os.path.realpath(run_directory)==os.path.realpath(working_directory)
    else:
        for x in run_directory:
            assert any([ y(os.path.join(working_directory,x)) for y in [os.path.isdir,os.path.isfile]]) , f"Not a file {x}"
        run_directory=[ os.path.join(working_directory,x) for x in run_directory]

    the_tar=f"{the_model.__name__}_{id}_WID-{worker_id}"
    if os.path.isdir(the_tar): run_shell_command(f"rm -r {the_tar}")
    if isinstance(run_directory,str):
        run_shell_command(f"cp -r {run_directory} {the_tar}")
        pack_files=run_directory
    else:
        if  len(run_directory)>0:
            os.mkdir(the_tar)
            run_shell_command( f"cp -r {' '.join(run_directory)} {the_tar}")
        else:
            raise Exception(f"")



    run_shell_command(f"tar --create --file={the_tar}.tar {the_tar} --remove-files")
    run_shell_command(f"xz {the_tar}.tar")
    compressed_file=f"{the_tar}.tar.xz"
    assert os.path.isfile(compressed_file), f"Expected to see file {os.path.realpath(compressed_file)} but does not exist"
    return compressed_file