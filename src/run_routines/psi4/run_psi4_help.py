from . import *
from qcp_objects.objects.properties import geometry
from orm_import.database_declaration import FCHK_File
from .psi4_helper import job_opts
from qcp_versioning.foreign_version import get_foreign_version

# make config
def config_base(
    tracker:Tracker,
    geom:geometry,
    record:Wave_Function,
) -> dict:
    """Pass data from wave function and geometry to psi4 config"""
    
    

    try:
        xyz_file=tracker.job_name
        xyz_file=geom.print_out(format='xyz', output_name=xyz_file)
    except Exception as ex: my_exception(f"Problem in generating geometry:", ex)
    try:
        method=record.method
        basis=record.basis
    except Exception as ex: my_exception(f"Problem in provided data",ex)
    try:
        pro=json.loads(record.protocol)
        dat=record.protocol_model(**pro)
        scf_type=dat.type
        freeze_core=dat.frozen_core
        reference=dat.reference
        cd_thres=dat.cd_thres
        df_basis=dat.df_basis
        dft_opts=dat.dft_settings
        convergence=dat.convergence
        if dat.fno_thres: # not zero or none
            do_fno=True
            fno_thres=dat.fno_thres
        else:
            do_fno=False
            fno_thres=0


    except Exception as ex: my_exception(f"Problem in parsing wave function specs: {record.protocol}", ex)
    
    return dict(
        geom=dict(
            ac_shift=None,
            charge=0,
            multiplicity=1,
            xyz_file=xyz_file,
        ),
        method=dict(
            **( dict(dft_opts=dft_opts.model_dump()) if dft_opts else {} ),
            **( dict(convergence=convergence.model_dump()) if convergence else {} ),
            method_tag=method,
            basis_set=basis,
            scf_type='_'.join([scf_type]+([df_basis] if scf_type.lower()=='df' else [])),
            freeze_core=freeze_core,
            reference=reference,
            fno=do_fno,
            fno_thres=fno_thres,
            cd_thres=cd_thres,
            do_moments=False,
        ),
    )

def machine_settings(tracker):
    return dict(
        machine_settings=dict(
            memory=f"{tracker.memory_GB}_GB",
            num_threads=tracker.num_threads,
        )
    )

from orm_import.database_declaration import *
def job_settings(job_tag:job_opts, record:SQLModel):
    if job_tag==job_opts.MOLPOL_FINITE_FIELD:
        assert isinstance(record, Molecular_Polarizability)
        specs=json.loads(record.specs)
        try:
            model=Molecular_Polarizability.specs_model_ff(**specs)
        except Exception as ex:
            my_exception(f"Problem in parsing molecular polarizability finite field specs: {specs}", ex)

        return dict(
            job_settings=dict(
                delta_dip=model.finfie_stepsize_dip,
                delta_qad=model.finfie_stepsize_qad,
                do_moments=False,
            )
        )
    elif job_tag==job_opts.MOLPOL_LINEAR_RESPONSE:
        assert isinstance(record, Molecular_Polarizability)
        return dict(
            job_settings=dict()
        )
    else:
        raise NotImplementedError(f"Job tag {job_tag} not implemented in job_settings")


CODE_ENTRY_DUMP="code_used.yaml"
@val_call 
def compute_core(
    python_exc:pdtc_file, psi4_script:pdtc_file,
    record:SQLModel,
    code:Code,
    super_record:SQLModel,
    geom:geometry,
    job_tag:job_opts,
    tracker: Tracker,
    extra_cmdln_opts: dict,
):

    try:
        local_version_run_psi4=get_foreign_version(python_exc, "run_psi4")
        assert code.code=='run_psi4', f"Expected code \'run_psi4\', got: {code.code}"
        assert local_version_run_psi4==code.version_hash, f"Version hash mismatch for run_psi4: local {local_version_run_psi4} vs. provided {code.version_hash}"
    except Exception as ex:
        raise Exception(f"Version mismatch for run_psi4: {ex}")

    config=config_base(tracker, geom, record)
    config.update( machine_settings(tracker) )
    config.update( job_settings(job_tag, super_record) )
    config.update(
        jobtag=job_tag.value,
    )

    config_file=f"config_{tracker.job_name}.yaml"
    with open(config_file,'w') as f:
        yaml.dump(config,f)
    
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


        ret=run_shell_command(cmd, [ config_file ], dict(exc=True, **extra_cmdln_opts))
        if ret.get('returncode', 0)!=0:
            tracker.add_error(ret.get('stderr',[]))
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
def recover_code() -> pdtc_file:
    code_file_search=f"{CODE_ENTRY_DUMP}"
    code_file=glob.glob(code_file_search)
    assert len(code_file)==1, f"Did not find exactely one code file at {os.getcwd()} for {code_file_search}: {code_file}"
    code_file=code_file[0]
    return code_file

@val_call
def get_code_entry() -> Code:
    code_file=recover_code()
    code_data=load_json_or_yaml(code_file)
    code_entry=Code.model_validate(code_data)
    return code_entry

    
    


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
