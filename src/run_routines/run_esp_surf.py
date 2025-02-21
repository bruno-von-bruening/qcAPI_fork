#!/usr/bin/env python

# import sys, os ; sys.path.insert(1, os.environ['SCR'])
# import modules.mod_utils as m_utl
from . import *
from util.environment import file

@validate_call
def setup_environment(tracker: Tracker, record_id: str|int, worker_id: str, fchk_file: file, grid_fis: List[file]):
    # Generate jobname and switch to designated work directory
    jobname=f"ESP_{make_jobname(record_id, worker_id)}"
    work_dir=make_dir(jobname)
    os.chdir(work_dir)

    # Check prerequisites
    assert os.path.isfile(fchk_file)
    for grid_fi in grid_fis:
        assert os.path.isfile(grid_fi), f"Not a file: {grid_fi}"
    # Create link to fchk file
    linked_file=f"ln_{os.path.basename(fchk_file)}"
    if os.path.islink(linked_file):
        os.unlink(linked_file)
    p=sp.Popen(f'ln -s {fchk_file} {linked_file}', shell=True)
    p.communicate()

    # Copy the grid_file
    for grid_fi in grid_fis:
        shutil.copy(grid_fi,'.')
    return tracker, work_dir, linked_file, [ os.path.basename(x) for x in grid_fis]
@validate_call
def setup_bash_env(tracker: Tracker, mode: str='fortran', shell_env=None, num_threads:int=1):
    export_lines=[f"export OMP_NUM_THREADS={num_threads}"]
    if mode in ['fortran']:
        assert isinstance(shell_env, dict), f"shell_env should be dict but is {shell_env}"
        #env_path_key='env_paths'
        #assert env_path_key in shell_env.keys(), f"key {env_path_key} is not in {shell_env.keys()}"
        #env_paths=shell_env[env_path_key]
        #assert isinstance(env_paths, dict)
        for key, value in shell_env.items():
            export_lines.append(f"export {key}={value}")
        return export_lines
    elif mode in ['python']:
        return export_lines
    else:
        raise Exception(f"unkown mode: {mode}")
    # the calculation is interfaced with fortran, hence we need to copy library file (or could also reference as hardcoded path)
    #for x in ['.f90','.F90', '.so', '.sh']:
    #    [ shutil.copy(y,'.') for y in glob.glob(os.path.dirname(script_exc)+f"/*{x}")  ]

@validate_call
def execute(Tracker: Tracker, python_exc:file, script_exc:file, linked_file:file, grid_fis:List[file], shell_lines:List[str]):
    grid_fis_str=' '.join(grid_fis)

    cmd=' ; '.join(shell_lines+[
        f"{python_exc} {script_exc} --dens {linked_file} --surf {grid_fis_str}"
    ])
    horton=sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, preexec_fn=os.setsid)
    stdout, stderr = horton.communicate()
    if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")
    
    return Tracker


def run_esp_surf(python_exc, script_exc, fchk_file, surface_file, record, worker_id, num_threads, target_dir, do_test,
        shell_env=None, mode='fortran',
):
    """ """
    starting_time=time.time()
    tracker=Tracker()
    record_id=record['id']
    grid_fis=[surface_file]

    ##
    ## autodelete fchk file?
    ##

    setup_lines=setup_bash_env(tracker, shell_env=shell_env, mode=mode, num_threads=num_threads)

    tracker,work_dir, dens_file, grid_fis=setup_environment(tracker, record_id, worker_id, fchk_file, grid_fis )

    tracker=execute(tracker, python_exc, script_exc, dens_file, grid_fis, shell_lines=setup_lines) 
    
    # Copy to target_dir
    if tracker.no_error:
        try:
            new_dir=os.path.join(target_dir, os.path.basename(work_dir))
            os.chdir('..')
            assert os.path.isdir(work_dir)
            cmd=f"cp -r {work_dir} {new_dir}"
            copy_dir=sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, preexec_fn=os.setsid)
            stdout, stderr = copy_dir.communicate()
            tracker.add_message(" and successful copying")
            if copy_dir.returncode!=0:
                raise Exception(f"Error in copying {stderr.decode()}")
        except Exception as ex:
            tracker.add_error(f"Error in copying files: {ex} {stderr.decode()}")
            tracker.add_message(' but failed copying')
            raise Exception(ex)
    
    # Recover results
    if tracker.no_error:
        try:
            os.chdir(os.path.realpath(new_dir))
            results_files='results.json'
            assert os.path.isfile(results_files), f"Not a file {os.path.realpath(results_files)}"

            with open(results_files, 'r') as rd:
                content=json.load(rd)
            try:
                esp_maps=content['files']['esp-maps']
                if len(esp_maps)!=1:
                    raise Exception(f"Expceted exactely one esp map, found {esp_maps}")
                esp_map=list(esp_maps.values())[0]
            except Exception as ex:
                raise Exception(f"Error in recovering esp maps: {str(ex)}")

            try:
                with open(esp_map,'r') as rd:
                    data=json.load(rd)
                    stats=data['statistics']
                assert isinstance(stats, dict), f"Excpected stats as dictionary, got {type(stats)}:\n{stats}"
            except Exception as ex:
                stats=None
                tracker.add_warning(f"Could not retrieve statistical indicators from {results_files}: {str(ex)}")
            if not isinstance(stats, type(None)):
                try:
                    stats=Map_Stats_Model(**stats)
                    stats=stats.model_dump()
                except Exception as ex:
                    tracker.add_warning(f"Failure in generating {Map_Stats_Model.__name__} fomr map_file=\'{esp_map}\':\n{ex}")
                    stats=None
            
            #with open(esp_map,'r') as rd:
            #    content=json.load(rd)
            #    scalars_bytes=np.array( content['surface_values'], dtype=np.float32 ).tobytes()
            #    with open('test.bytes','wb') as wr:
            #        wr.write(scalars_bytes)
            esp_map_di={
                'file_name' : os.path.basename(esp_map),
                'hostname'  : os.uname()[1],
                'path_to_container' : os.path.relpath(os.path.realpath(os.path.dirname(esp_map)), os.environ['HOME']),
                'path_in_container' : '.',
            }

            run_data={'map_file':esp_map_di, 'stats': stats}
            converged=1
        except Exception as ex:
            converged=0
            run_data=None
            if do_test:
                raise Exception(f"Error in recovering results: {str(ex)}")
    
    record.update({"converged":converged, **tracker.model_dump()})
    run_info={'status':tracker.status, 'status_code':tracker.status_code}
    record.update({"run_data":run_data, 'run_info':run_info})
    return record
    

    