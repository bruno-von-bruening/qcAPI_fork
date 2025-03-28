# NEW
import os, sys, shutil, json, re, copy
import subprocess
import time, datetime
import numpy as np

from . import *

def psi4_copy(file, target_dir=None):
    if not isinstance(file, str):
        raise Exception(f"You did not provide string: {file}")
    elif not os.path.isfile(file):
        raise Exception(f"Requested to copy {file}, but is not a file.")
    elif not isinstance(target_dir, type(None)):
        print(f"Copying {file} to {target_dir}")
        if not os.path.dirname(os.path.realpath(file))==os.path.realpath(target_dir):
            try:
                p=subprocess.Popen(f"cp {file} {target_dir}", shell=True)
                p.communicate()
                return  os.path.join(target_dir, os.path.basename(file))
            except Exception as ex:
                raise Exception(f"Problem when trying to copy file: {ex}")

def make_size_entry(file,format='MB'):
    def bytes_to_mb(bytes):
        conversion=1024
        return bytes/conversion**2
    size=os.path.getsize(file)
    if format=='MB':
        size=bytes_to_mb(size)
    return {
        'file_name':file,
        'size_format':format,
        'size':size,
    }

def decompress_file(file):
    assert os.path.isfile(file), f'Not a file: {file}'


    extension_to_command={
        'gz': '/usr/bin/gunzip -f',
        '7z': '/usr/bin/7za x -aoa',
        'xz': '/usr/bin/xz --decompress',
    }
    extension=file.split('.')[-1]
    extension_lower=extension.lower()
    if not extension_lower in list(extension_to_command.keys()):
        print(f"I do not know extension of file {file}")
        return
    else:
        try:
            command=f"{extension_to_command[extension_lower]} {file}"
            p=subprocess.Popen(command)
            p.communicate()
            new_file=file.replace(f'.{extension}', '')
            if not os.path.isfile(new_file):
                raise Exception(f"There should be gzipped file: {new_file} (after running {command})")
        except Exception as ex:
            print(f"Could not decompress: {ex} (for {file})")


def compress_file(file, format='MB', encoding=None, decompress=False):
    
    assert os.path.isfile(file), f'Not a file: {file}'

    dic={}
    if encoding=='regular' or encoding==None:
        entry=make_size_entry(file)
        dic.update({'regular':entry})
    else:
        ar=encoding.split('|')
        encoding_type=ar[0]
        if len(ar)==1:
            encoding_level=''
        elif len(ar)==2:
            encoding_level=ar[1]
        else:
            raise Exception()
        
        if encoding_type.startswith('gz'):
            extension='.gz'
            compressed_file=file+extension
            command=f'gzip -f{encoding_level} {file}'
        elif encoding_type.startswith('7z'):
            extension='.7z'
            compressed_file=file+extension
            command=f'7za a {compressed_file} {file} -mx{encoding_level}'
        elif encoding_type.startswith('xz'):
            extension='.xz'
            compressed_file=file+extension
            command=f'xz {file} -{encoding_level}'
        else:
            raise Exception()

        try:
            p=subprocess.Popen(command, shell=True)
            p.communicate()
            if not os.path.isfile(compressed_file):
                raise Exception(f"There should be gzipped file: {compressed_file}")
            entry=make_size_entry(compressed_file)
            entry.update({'compression_command':command})
            dic.update({encoding:entry})
        except Exception as ex:
            print(f"Could not compress: {ex}")
            dic.update({encoding:None})
    return dic




def get_system_data():
    import socket
    hostname=os.popen("hostname").read().strip()
    nprocs=os.popen("nproc --all").read().strip()
    avail_mem_gb=os.popen("awk \'/MemFree/ { printf \"%.3f \\n\", $2/1024/1024 }\' /proc/meminfo").read().strip()
    mem_info=os.popen("cat /proc/meminfo").read()
    usage_info=os.popen("top -bn1 | head -n20").read()
    cpu_info=os.popen("lscpu").read()

    system_data=dict(
        hostname=hostname,
        number_of_threads=nprocs,
        total_memory_in_gb=avail_mem_gb,
        mem_info=mem_info,
        processes_info=usage_info,
        cpu_info=cpu_info
    )

    return system_data

def psi4_after_run(psi4_dict, target_dir=None, gzip=True, delete=True):
    # Store the results here
    info_dic={'size':{},'timing':{}}
    done_grac=psi4_dict['settings']['grac_correction']
    ##### Anaylse length
    ####################
    def get_timings(output_file):
        if isinstance(output_file,type(None)):
            return None
        else:
            with open(output_file,'r') as rd:
                lines=rd.readlines()
            timing_tag='TIMING'
            timing_lines=[x for  x in lines if x.startswith(timing_tag)]
            if done_grac:
                expected_length=6
            else:
                expected_length=2
            assert len(timing_lines)==expected_length, f"Expected {expected_length} lines, not {len(timing_lines)}:\n{timing_lines}, {output_file}"
            timing_lines_secs=[ x for x in timing_lines if bool(re.search(r'\[in seconds\]',x))]
            di={}
            for line in timing_lines_secs:
                try:
                    tag=line.split()[1]
                    num=line.split()[-1]
                    num=float(num)
                except Exception as ex:
                    raise Exception(f"On line {line}: {ex}")
                di.update({tag:num})
            return di
    output_file=psi4_dict['psi4out_file']
    timings=get_timings(output_file)
    info_dic['timing'].update(timings)
    
    ##### Get size, gzip, copy
    ######################
    test_compression=False
    n=['regular'] ; 
    if test_compression:
        a=['regular', 'gz|3','gz|9','gz|6' , '7z|3','7z|9', '7z|6' , 'xz|3','xz|9', 'xz|6']
    else:
        a=copy.deepcopy(n)
    zip_switch={ # one is false zero is true
        'psi4inp_file'      : n,
        'psi4out_file'      : n,
        'wfn_grac_file'     : a,
        'fchk_grac_file'    : a,
        'wfn_neut_file'     : a,
        'fchk_neut_file'    : a,
        'final_fchk_file'   : a,
    }
    new_files={}
    if done_grac:
        file_keys=['psi4inp_file', 'psi4out_file', 'fchk_grac_file',  'fchk_neut_file']
    else:
        file_keys=['psi4inp_file', 'psi4out_file',  'fchk_neut_file']
    for tag in file_keys:#,'wfn_grac_file', 'wfn_neut_file']:
        # Get the associated keys
        assert tag in psi4_dict.keys() and zip_switch.keys()
        file=psi4_dict[tag]

        if tag in [ 'wfn_grac_file', 'wfn_neut_file' ]:
            p=subprocess.Popen(f"rm {file}", shell=True)
            p.communicate()
        else:
            if not isinstance(file, type(None)):
                requested_encodings=zip_switch[tag]
                short_tag=tag.replace('_file','')

                for encoding in requested_encodings:
                    dic=compress_file(file, encoding=encoding)
                    info_dic['size'].update(dict([
                        (f"{short_tag}_{k}",v) for k,v in dic.items()
                    ]))
                    # Copy the file to results dir
                    new_file=dic[encoding]['file_name']
                    assert isinstance(new_file,str), f"file {new_file} not valid"
                    if not isinstance(target_dir, type(None)):
                        new_file_location=psi4_copy(new_file, target_dir=target_dir)
                        new_files.update({tag:os.path.realpath(new_file_location)})
                    # Decompress the file in case it has been compresssed
                    if not encoding in ['regular', None]:
                        decompress_file(new_file)
                for key in info_dic['size'].keys():
                    if delete:
                        try:
                            file=info_dic['size'][key]['file_name']
                            if os.path.isfile(file):
                                p=subprocess.Popen(f"rm {file} -r", shell=True)
                                p.communicate()
                        except Exception as ex:
                            print(f"Could not delete file {file}")

    return info_dic, new_files

def make_psi4_input_file(make_psi4_input, 
            atom_types, coordinates, method, basis_set,
            total_charge=0, multiplicity=1,
            jobname='test', hardware_settings=None,
            target_dir=None):
    """ Interface between this data and my generic run script """
    psi4_start_time=time.time()
    
    # Make xyz_file
    xyz_file=jobname+'.xyz'
    with open(xyz_file,'w') as wr:
        lines=[ f"{len(atom_types)}", "Generated for running job: \'{jobname}\'"]
        lines+=[    "{at_ty:<2} {coord_str}".format(
                            at_ty=at, coord_str=' '.join([f"{x:>12}" for x in coor])
                        )
                        for at, coor in zip(atom_types, coordinates)
        ]
        wr.write('\n'.join(lines)+'\n')



    # Generate the input file
    job='single_point'
    files_per_calc=make_psi4_input(job, func=method, basis_set=basis_set, xyz_file=xyz_file, execute=False, add_method_tags=False)
    assert len(files_per_calc)==1
    psi4_input_file=files_per_calc[0]['input']
    psi4_storage_file=files_per_calc[0]['storage']

    return psi4_input_file, psi4_storage_file

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
    assert os.path.realpath(target_dir)!=os.path.realpath(local_dir)
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
                p=subprocess.Popen(f"xz -6 {fi}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = p.communicate()
                if p.returncode != 0:
                    print(f"Errror when compressing {fi}: {stderr.decode()}")
                else:
                    xz_file=fi+'.xz'
                    assert os.path.isfile(xz_file)
                    fi=xz_file
            p=subprocess.Popen(f"cp -r {fi} {target_dir}", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr=p.communicate()
            if p.returncode!=0:
                raise Exception(stderr.decode())
        except Exception as ex:
            print(f"error in copying {fi}: {str(ex)}")
    new_fchk_file=os.path.join( target_dir , os.path.basename(psi4_dict['final_fchk']) )+'.xz'
    new_storage_file=os.path.join( target_dir , os.path.basename(storage_file) )
    assert os.path.isfile(new_fchk_file)
    assert os.path.isfile(new_storage_file)
    return dict(
        fchk_file=os.path.realpath(  new_fchk_file ),
        storage_file=os.path.realpath( new_storage_file)
    )

def compute_wave_function(conda_env, psi4_script, record, workder_id, num_threads=1, max_iter=150, target_dir=None, do_test=False, geom=None):
    """ Cal psi4 calculation
    This routine only processes """
    start_time = time.time()

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
        atom_types=[ nuclear_charge_to_element_symbol(x) for x in species ]
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
        error=None
        my_sep('SUCCESS in  psi4 calculation','#')
    except Exception as ex:
        # In case of a test we want errors to kill the job so we notice them immediately
        converged=0
        error=str(ex)
        fchk_file=None
        # Get hostname/ho
        hostname=os.popen("hostname").read().strip()
        my_sep('FAILURE in  psi4 calculation ','!',info_line=f"ERROR: {error}", sep_times=2)
        if do_test:
            raise Exception(f"Test was requested hence terminating:\n{ex}")
    
    fchk_info={
        'hostname': os.uname()[1],
        'path_to_container': os.path.relpath(os.path.dirname(fchk_file), os.environ['HOME']),
        'path_in_container': '.',
        'file_name': os.path.basename(fchk_file),
        'size_mb': os.path.getsize(fchk_file)/(1024**2)
    }
    ### RETURN the ouptu
    # Immutable information
    output=dict(**record)
    output.update( dict(
        fchk_info=fchk_info,
        energy=energy,
        energy_gradient=gradient,
    ))
    # Variable information, consider that this has to be the same otherwise the created unique identifier will be different
    elapsed_time=time.time()-start_time
    output.update(dict(
        elapsed_time=elapsed_time,
        converged=converged,
        error=error,
        message=None,
        warning=None
    ))
    return output
