from ..basic_imports import *

@val_call 
def compute_core(
    python_exc:pdtc_file, psi4_script:pdtc_file,
    record:SQLModel,geom:geometry,
    job_tag:str,
    tracker: Tracker,
):

    try:
        method=record.method
        basis=record.basis
    except Exception as ex: my_exception(f"Problem in provided data",ex)

    try:
        xyz_file=tracker.job_name
        xyz_file=geom.print_out(format='xyz', output_name=xyz_file)
    except Exception as ex: my_exception(f"Problem in generating geometry:", ex)
    
    # Generic
    try:
        cmd=f"{python_exc} {psi4_script}"
        my_sep('START psi4 calculation',tracker.job_name,'+')
        run_shell_command(cmd, [ job_tag], dict(func=method, basis=basis, xyz=xyz_file, exc=True, num_thread=tracker.num_threads))
    except Exception as ex: my_exception(f"Problem in running psi4 job:", ex)

    return record, tracker

@val_call
def recover_storage(jobname) -> pdtc_file:
    storage_file_search=f"STORAGE_*{jobname}.yaml"
    storage_file=glob.glob(storage_file_search)
    assert len(storage_file)==1, f"Did not find exactely one storage file at {os.getcwd()} for {storage_file_search}: {storage_file}"
    storage_file=storage_file[0]
    return storage_file

@val_call
def recover_specific(storage_file, tag):

    storage_data=load_json_or_yaml(storage_file)
    files=dict(
        storage_file=storage_file,
    )
    sub_entries={}
    
    if tag == 'single_point':
        assert 'files' in storage_data, f"Key \'files\' not in storage file {storage_file}"
        assert 'final_fchk' in storage_data['files'].keys(), f"Key \'final_fchk\' not in \'files\' section of {storage_file}"
        fchk_file=storage_data['files']['final_fchk']

        compresssed_fchk_file=compress_file(fchk_file, compression_type='xz',compression_level=None)
        files={
            FCHK_File.__name__:os.path.realpath(compresssed_fchk_file),
        }
        sub_entries={}
    
    


    return files, sub_entries