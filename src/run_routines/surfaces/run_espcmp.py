from . import *

@validate_call
def setup_environment(tracker: Tracker,record_id:str|int, worker_id:str, dmp_map_file:file, rho_map_file:file):
    """ """
    
    # Generate jobname and switch to designated work directory
    jobname=make_jobname(record_id, worker_id, job_tag='ESPCMP')
    work_dir=make_dir(jobname)
    os.chdir(work_dir)

    ln_dmp_map_file=temporary_file(dmp_map_file)
    ln_rho_map_file=temporary_file(rho_map_file)

    return jobname, work_dir, ln_dmp_map_file, ln_rho_map_file

@validate_call
def execution(python_exc:file, python_script:file, dmp_map_file:file, rho_map_file:file):
    cmd=f"{python_exc} {python_script} --trial {dmp_map_file} --ref {rho_map_file}"
    stdout, stderr = run_shell_command(cmd)
    return stdout, stderr

@validate_call
def recover_output(
    tracker: Tracker, results_file:file='results.yaml'
) -> Tuple[Tracker, dict]:
    
    with open(results_file, 'r') as rd:
        results=yaml.safe_load(rd)
    map_file=results['files']['map']
    stats=results['results']['statistical_indicators']

    # Convert file to file_path
    hostname=os.uname()[1]
    path_to_container=os.path.relpath(os.path.realpath(os.path.dirname(map_file)), os.environ['HOME'])
    path_in_container=os.path.relpath(os.path.realpath(os.path.dirname(map_file)), os.path.join(os.environ['HOME'],path_to_container))
    file_name=os.path.basename(map_file)
    #map_file=dict(hostname=hostname, path_to_container=path_to_container, path_in_container=path_in_container, file_name=file_name)

    run_data=dict(
        files={
            DMP_vs_RHO_MAP_File.__name__:os.path.realpath(map_file)},
        sub_entries={
            DMP_vs_RHO_MAP_Stats.__name__: stats,
        }
    )
    working_dir=os.getcwd()
    files_to_store=get_relevant_files(working_dir,run_data)
    run_data.update(dict(
        working_dir=working_dir,
        files_to_store=files_to_store,
    ))
    return tracker, run_data

# Validate call does not work for objects that are called with multiprocessing functions
#@validate_call
def run_espcmp(python:file, script:file, dmp_map_file:file, rho_map_file:file, 
               record: dict, worker_id:str, 
               num_threads:int=1, target_dir:directory|None=None, do_test:bool=False
):
    tracker=Tracker()

    # make directory
    # copy the things into directory (maybe as link)
    # Execute the script
    # recover the results
    record_id=record['id']
    try:
        try:
            jobname, work_dir, ln_dmp_map_file, ln_rho_map_file = setup_environment(tracker, record_id, worker_id, dmp_map_file=dmp_map_file, rho_map_file=rho_map_file)
        except Exception as ex: raise Exception(f"Failed to prepare data: {ex}")

        try:
            stdout, stderr = execution(python, script, dmp_map_file=ln_dmp_map_file.file, rho_map_file=ln_rho_map_file.file)
        except Exception as ex: raise Exception(f"Failed to execute function: {ex}")

        try:
            tracker, run_data=recover_output(tracker)
        except Exception as ex: raise Exception(f"Failed to recover results: {ex}")

        converged=RecordStatus.converged
    except Exception as ex:
        tracker.add_error(ex)
        converged=RecordStatus.failed
        run_data=None        

    record.update({'converged':converged, 'run_data':run_data, **tracker.model_dump() })
    return record
