from . import *
from qcp_objects.objects.properties import geometry
from orm_import.database_declaration import FCHK_File

@val_call 
def compute_core(
    python_exc:pdtc_file, psi4_script:pdtc_file,
    record:SQLModel,geom:geometry,
    job_tag:str,
    tracker: Tracker,
    extra_cmdln_opts: dict,
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

        # Default for psi4 threads
        if tracker.num_threads is None:
            num_threads=4
            warn(f"No number of threads specified, using default: {tracker.num_threads}")
        else:
            num_threads=tracker.num_threads

        # Default for psi4 memory
        if tracker.memory_GB is not None:
            mem=tracker.memory_GB
        else:
            mem_per_thread_GB=2
            mem=tracker.num_threads*mem_per_thread_GB
            warn(f"No memory specified, using default ({mem_per_thread_GB} GB per thread): {mem} GB")
        run_shell_command(cmd, [ job_tag], dict(
            func=method, basis=basis, geom=xyz_file, exc=True, 
            num_thread=num_threads, memory=f"{mem}_GB",
        )| extra_cmdln_opts)
    except Exception as ex: my_exception(f"Problem in running psi4 job:", ex)

    return record, tracker

@val_call
def recover_storage(jobname) -> pdtc_file:
    storage_file_search=f"STORAGE_*{jobname}.yaml"
    storage_file=glob.glob(storage_file_search)
    assert len(storage_file)==1, f"Did not find exactely one storage file at {os.getcwd()} for {storage_file_search}: {storage_file}"
    storage_file=storage_file[0]
    return storage_file

    
    


def open_storage_file(storage_file:pdtc_file) -> dict:
    storage_data=load_json_or_yaml(storage_file)
    return storage_data

@val_call
def get_from_storage(storage_file:pdtc_file, path:List[str]|str):
    if isinstance(path,str):
        path=[path]
    storage_data=open_storage_file(storage_file)
    for i,p in enumerate(path):
        assert p in storage_data.keys(), f"Expected key '{p}' in storage data under path ({' -> '.join(path[:i+1])}) from file {storage_file}"
        storage_data=storage_data[p]
    return storage_data

@val_call
def get_fchk_file(tracker:Tracker,storage_file:pdtc_file,id):
    files=get_from_storage(storage_file, 'files')
    assert 'final_fchk' in files.keys(), f"Key \'final_fchk\' not in \'files\' section of storage file: {storage_file}"
    fchk_file=files['final_fchk']

    if fchk_file is None:
        tracker.add_warning(f"No fchk file available (that may happen for some specific methods in psi4")
        return tracker,{}

    compresssed_fchk_file=compress_file(fchk_file, compression_type='xz',compression_level=None)
    sub_entries={
        FCHK_File.__name__: FCHK_File(
            id=id,
            path_to_container='',
            path_in_container='',
            file_name= os.path.realpath(compresssed_fchk_file),
        )
    }
    return tracker, sub_entries
