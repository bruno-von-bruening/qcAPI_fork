# NEW
from . import *
import os, sys, shutil, json, re, copy
import numpy as np


from util.trackers import message_tracker

from qcp_objects.objects.properties import geometry

@val_call
def make_geom( 
            atom_types: List[str], coordinates:List[Tuple[float,float,float]],
            xyz_file_name: str,
):
    """ Interface between this data and my generic run script """
    xyz_file=xyz_file_name.replace('.xyz','')+'.xyz'
    geom=geometry(coordinates=np.array(coordinates), atom_types=atom_types, length_units='BOHR')
    xyz_file_name=geom.to_xyz_file(xyz_file)
    return xyz_file_name

def execute_the_job(conda_env, execute_psi4, psi4_input_file, psi4_storage_file):
    # Import the run script

    psi4_start_time=time.time()

    info= execute_psi4(psi4_input_file, conda_env=conda_env)
    stdout=info['stdout']
    stderr=info['stderr']
    return_code=info['return_code']

    # Confirm all expected results are there
    expected_files={
        'psi4inp_file': psi4_input_file,
        'psi4out_file': info['psi4_output_file'],
        'log_file': psi4_input_file.split('.')[0]+'.log',
        'storage_file': psi4_storage_file,
    }
    for key, file in expected_files.items():
        if not os.path.isfile(file):
            raise Exception(f"Not a file: {file}")
    
    with open(expected_files['storage_file'], 'r') as rd:
        import yaml
        data=yaml.safe_load(rd)
    computed_files=data['files']
    fchk_key='final_fchk'
    assert fchk_key in computed_files.keys(), f"Found no key {fchk_key}"
    fchk_file=computed_files[fchk_key]
    assert  os.path.isfile(fchk_file)

    # Recover the data
    psi4_dict=dict(
        exit_ana= None,#
        real_time=time.time()-psi4_start_time,
        time_unit='s',
        return_code=return_code,
        stdout= stdout,
        stderr= stderr,
        final_fchk=fchk_file,
    )
    return psi4_dict
    
def extract_psi4_info(wfn_file, method, basis):
    """ Interface with generic run script """
    psi4_dict=property_calc(wfn_file, method, basis)
    return psi4_dict

def run_compute_wave_function(jobname, conda_env, psi4_script, psi4_di, target_dir):
    """ Actual steps to be done:
    1.1 create a target directory for the results
    1.2 create a local directory mirroring the target directory
    2.1 create an input file
    2.2 execute the input file
    2.3 retrieve the information
    """
    # Make a target direcotry for the job (where things will be copied to)
    # If that does not work the calculation should not be started
    target_dir=make_dir(jobname, base_dir=target_dir)
    target_dir=os.path.realpath(target_dir)

    # Creates a local directory in which the files will be created
    local_dir=os.path.basename(target_dir)
    assert os.path.realpath(target_dir)!=os.path.realpath(local_dir), f"Local directory and target directory are the same!"
    if os.path.isdir(local_dir):
        raise Exception(f"Directory {local_dir} already exist, that should not happen")
    else:
        os.mkdir(local_dir)
        os.chdir(local_dir)


    def clean_psi4_manually():
        # Psi4 jobs are named after linux process ids
        pid=os.getpid()
        clean_file=f"psi.{pid}.clean"
        if not os.path.isfile(clean_file):
            print(f"Cannot find file {clean_file}, hence will not clean")
        else:
            # Get file (each is a line in this files)
            with open(clean_file, 'r') as rd:
                lines=rd.readlines()
                files=[l.strip() for l in lines]
            # Iterate over files and remove them
            for f in files:
                if not os.path.isfile(f):
                    print(f"Found {f} in {clean_file}: Expected file but not a valid file")
                else:
                    os.remove(f)

    # Import the run script
    def get_run_script(psi4_script):
        import importlib
        assert os.path.isfile(psi4_script), f"Psi4 script is not valid: {psi4_script}"
        sys.path.insert(1, os.path.dirname(psi4_script))
        run_psi4_mod=importlib.import_module(os.path.basename(psi4_script).split('.')[0])
        run_psi4=run_psi4_mod.run_psi4
        execute_psi4=run_psi4_mod.execute_psi4_shell
        return run_psi4, execute_psi4
    make_psi4_input, execute_psi4=get_run_script(psi4_script)

    # Get the wave function
    input_file, storage_file = make_psi4_input_file(make_psi4_input, **psi4_di)
    
    psi4_dict=execute_the_job(conda_env, execute_psi4, input_file, storage_file)

    # In case the job fails clean manually and raise error
    if psi4_dict['return_code']!=0:
        # Delete files (in case of a failed job this is not done automatically)
        clean_psi4_manually()
        raise Exception(f"Psi4 wave function run did terminate with error: {psi4_dict['stderr']}")
    
    # copy all files
    
    import glob
    for fi in glob.glob('*'):
        try:
            # if its a fchk file compress it first
            if fi.lower().endswith(f".fchk"):
                try:
                    stdout, stderr = run_shell_command(f"xz -6 {fi}")
                    xz_file=fi+'.xz'
                    assert os.path.isfile(xz_file)
                    fi=xz_file
                except Exception as ex: print(f"Could not xzip '{fi}': {str(ex)}")
            stdout, stderr=run_shell_command(f"cp -r {fi} {target_dir}")
        except Exception as ex:
            raise Exception(f"error in copying {fi}: {str(ex)}")
    new_fchk_file=os.path.join( target_dir , os.path.basename(psi4_dict['final_fchk']) )+'.xz'
    new_storage_file=os.path.join( target_dir , os.path.basename(storage_file) )
    assert os.path.isfile(new_fchk_file)    , f"Not a file: {new_fchk_file}"
    assert os.path.isfile(new_storage_file) , f"Not a file: {new_storage_file}"
    return dict(
        fchk_file=os.path.realpath(  new_fchk_file ),
        storage_file=os.path.realpath( new_storage_file)
    )

def compute_wave_function_old(conda_env, psi4_script, record, workder_id, num_threads=1, max_iter=150, target_dir=None, do_test=False, geom=None):
    """ Cal psi4 calculation
    This routine only processes """
    start_time = time.time()
    tracker=Tracker()

    def my_sep(message, sep, info_line=None, sep_times=1, print_hostname=True):
        """ Create headline to indicate status in output file (for orientation/debugging purposes)"""
        sep*=60
        if print_hostname:
            hostname=os.popen("hostname").read().strip()
            host_str=f" ( hostname=\'{hostname}\' )"
        else:
            host_str=''
        lines= [sep]*sep_times + [message + f" ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}){host_str}:"] + [' '*4+f"jobname=\'{jobname}\'"]
        if info_line!=None:
            lines+=[' '*4+info_line]
        lines+=[sep]*sep_times 
        print('\n'.join(lines))
    def process_conformation(geom, do_test=False):
        # Conformation data
        if do_test:
            coordinates=[[0,0,0],[0,0,1]]
            species=[1,1]
            multiplicity=1
            total_charge=0
        else:
            coordinates = geom["coordinates"]
            species = geom["nuclear_charges"]
            multiplicity=geom['multiplicity']
            total_charge=geom['charge']
        if isinstance(species[0],int):
            atom_types=[ nuclear_charge_to_element_symbol(x) for x in species ]
        elif isinstance(species[0],str):
            atom_types=species
        else: raise Exception(f"Unkown atom types: {atom_types}")
        return {
            'atom_types':atom_types,
            'coordinates': coordinates,
            'total_charge': total_charge,
            'multiplicity': multiplicity
        }
        

    # create ID
    id=record['id']
    jobname=make_jobname(id, workder_id)
    my_sep('START psi4 calculation','+')

    # Get input information
    psi4_di={}
    # Get conformational details (some are processed), possibly do a test by using Hydrogen molecule
    conf_di =process_conformation(geom, do_test=do_test)
    # Method to be run on conformation
    
    # Hardware settings
    hardware_settings=dict(
        num_threads=4,
        memory='8GB',
    )

    # Check that all keys are there
    [ psi4_di.update(x) for x in [conf_di]]
    psi4_di.update({'hardware_settings':hardware_settings, 'jobname':jobname})
    mandatory_keys=[ 'atom_types', 'coordinates']
    psi4_di.update({'method':record['method'], 'basis_set':record['basis']})
    optional_keys=['total_charge', 'multiplicity',
            'method', 'basis_set', 'jobname', 'hardware_settings', 'target_dir'
    ]
    assert all([x in psi4_di.keys() for x in mandatory_keys]), f"{psi4_di.keys()} {mandatory_keys}"
    assert all([x in mandatory_keys+optional_keys for x in psi4_di.keys()]), f"{psi4_di.keys()} {optional_keys+mandatory_keys}"
    
    try:
        return_dict=run_compute_wave_function(jobname, conda_env,psi4_script, psi4_di, target_dir)
        fchk_file=return_dict['fchk_file']
        storage_file=return_dict['storage_file']
        assert os.path.isfile(storage_file)
        with open(storage_file, 'r' ) as rd:
            data=yaml.safe_load(rd)
            energy=data['results']['energy']
            gradient=data['results']['gradient']

        # If everything gone well return positive error code
        converged=1
        my_sep('SUCCESS in  psi4 calculation','#')
    except Exception as ex:
        # In case of a test we want errors to kill the job so we notice them immediately
        converged=0
        tracker.add_error(ex)
        fchk_file=None
        energy=None
        gradient=None
        # Get hostname/ho
        hostname=os.popen("hostname").read().strip()
        tracker.add_error( my_sep('FAILURE in  psi4 calculation ','!',info_line=f"ERROR: {str(ex)}", sep_times=2) )
        do_test=True
        if do_test:
            raise Exception(f"Test was requested hence terminating:\n{ex}")
    
    #path_to_container=os.path.relpath(os.path.dirname(fchk_file), os.environ['HOME']) if not fchk_file is None else 'no file produced'
    #file_name= os.path.basename(fchk_file) if fchk_file is not None else 'no file produced'
    #size_mb=os.path.getsize(fchk_file)/(1024**2) if fchk_file is not None else -1.
    #fchk_info={
    #    'hostname': os.uname()[1],
    #    'path_to_container': path_to_container,
    #    'path_in_container': '.',
    #    'file_name': file_name,
    #    'size_mb': size_mb,
    #}
    ### RETURN the ouptu
    to_store=[
        os.path.realpath(x) for x in glob.glob('*') if not ( bool(re.search(r'.fchk',x) or os.path.islink(x) )) 
    ]
    # Immutable information
    output=dict(**record)
    run_data=dict(
        files={
            FCHK_File.__name__:fchk_file,
        },
        run_directory=os.getcwd(),
        to_store=to_store,
    )
    output.update( dict(
        run_data=run_data,
        energy=energy,
        energy_gradient=gradient,
    ))
    
    # Variable information, consider that this has to be the same otherwise the created unique identifier will be different
    elapsed_time=time.time()-start_time
    output.update(dict(
        #elapsed_time=elapsed_time,
        converged=converged,
        **tracker.model_dump()
    ))
    return output


def compute_wave_function(python, psi4_script, record, workder_id, num_threads=1, max_iter=150, target_dir=None, do_test=False, geom=None):
    start_time = time.time()
    tracker=Tracker()
    
    def my_sep(message, sep, info_line=None, sep_times=1, print_hostname=True):
        """ Create headline to indicate status in output file (for orientation/debugging purposes)"""
        sep*=60
        if print_hostname:
            hostname=os.popen("hostname").read().strip()
            host_str=f" ( hostname=\'{hostname}\' )"
        else:
            host_str=''
        lines= [sep]*sep_times + [message + f" ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}){host_str}:"] + [' '*4+f"jobname=\'{jobname}\'"]
        if info_line!=None:
            lines+=[' '*4+info_line]
        lines+=[sep]*sep_times 
        print('\n'.join(lines))
    #def process_conformation(geom, do_test=False):
    #    # Conformation data
    #    if do_test:
    #        coordinates=[[0,0,0],[0,0,1]]
    #        species=[1,1]
    #        multiplicity=1
    #        total_charge=0
    #    else:
    #        coordinates = geom["coordinates"]
    #        species = geom["nuclear_charges"]
    #        multiplicity=geom['multiplicity']
    #        total_charge=geom['charge']
    #    if isinstance(species[0],int):
    #        atom_types=[ nuclear_charge_to_element_symbol(x) for x in species ]
    #    elif isinstance(species[0],str):
    #        atom_types=species
    #    else: raise Exception(f"Unkown atom types: {atom_types}")
    #    return {
    #        'atom_types':atom_types,
    #        'coordinates': coordinates,
    #        'total_charge': total_charge,
    #        'multiplicity': multiplicity
    #    }
        

    # create ID
    try:
        try:
            id=record['id']
            method=record['method']
            basis=record['basis']
        except Exception as ex: my_exception(f"Problem in provided data",ex)

        try:
            jobname=make_jobname(id, workder_id)
            working_dir=make_dir(jobname=jobname)
            os.chdir(working_dir)
        except Exception as ex: my_exception(f"Problem in setting up environment", ex)

        try:
            available_keys=list(geom.keys())
            requested_keys=['nuclear_charges','coordinates']
            keys_not_there=[ k for k in requested_keys if k not in available_keys]
            if len(keys_not_there)>0: raise Exception(f"Could not find keys {keys_not_there} in {available_keys}")

            xyz_file=make_geom(atom_types=geom['nuclear_charges'], coordinates=geom['coordinates'], xyz_file_name=jobname)
        except Exception as ex: my_exception(f"Problem in generating geometry:", ex)


        try:
            job_tag='single_point'
            cmd=f"{python} {psi4_script} {job_tag}"\
                +f" --func {method} --basis {basis} --xyz {xyz_file}" \
                +f" --exc --num_threads {num_threads}"
            my_sep('START psi4 calculation','+')
            run_shell_command(cmd)
        except Exception as ex: my_exception(f"Problem in running psi4 job:", ex)

        try: # Postprocessing
            # Storage file:
            storage_file_search=f"STORAGE_*{jobname}.yaml"
            storage_file=glob.glob(storage_file_search)
            assert len(storage_file)==1, f"Did not find exactely one storage file at {os.getcwd()} for {storage_file_search}: {storage_file}"
            storage_file=storage_file[0]
            assert len(storage_file)
            assert os.path.isfile(storage_file), f"Not a file {storage_file}"
            with open(storage_file, 'r') as rd:
                storage_data=yaml.safe_load(rd)

            assert 'files' in storage_data, f"Key \'files\' not in storage file {storage_file}"
            assert 'final_fchk' in storage_data['files'].keys(), f"Key \'final_fchk\' not in \'files\' section of {storage_file}"
            fchk_file=storage_data['files']['final_fchk']

        except Exception as ex: my_exception(f"Problem in recovering results",ex)

        converged=1
        compresssed_fchk_file=compress_file(fchk_file, compression_type='xz',compression_level=None)
        files={
            FCHK_File.__name__:os.path.realpath(compresssed_fchk_file),
        }
        sub_entries=None
        my_sep('SUCCESS in psi4 calculation','#')
    except Exception as ex:
        converged=0
        files=None
        sub_entries=None
        tracker.add_error(ex)
        my_sep('FAILED psi4 calculation','#')
        if do_test:
            raise Exception(f"Test was requested hence terminating:\n{ex}")


    
    run_info={'status':tracker.status, 'status_code':tracker.status_code}
    run_data=dict(
        sub_entries=sub_entries,
        files=files,
        run_directory=working_dir,
        to_store=working_dir,
    )
    record.update({'converged':converged, **tracker.model_dump()})
    record.update({'run_data':run_data, 'run_info':run_info})

    return record
    