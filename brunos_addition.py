
# NEW
import os, sys, shutil, json, re, copy
from run_psi4_2 import complete_calc, psi4_exit_ana, property_calc
import subprocess
import time, datetime
import psi4
import numpy as np
from functools import partial

# Replacers for Thomas script
PERIODIC_TABLE_STR = """
H                                                                                                                           He
Li  Be                                                                                                  B   C   N   O   F   Ne
Na  Mg                                                                                                  Al  Si  P   S   Cl  Ar
K   Ca  Sc                                                          Ti  V   Cr  Mn  Fe  Co  Ni  Cu  Zn  Ga  Ge  As  Se  Br  Kr
Rb  Sr  Y                                                           Zr  Nb  Mo  Tc  Ru  Rh  Pd  Ag  Cd  In  Sn  Sb  Te  I   Xe
Cs  Ba  La  Ce  Pr  Nd  Pm  Sm  Eu  Gd  Tb  Dy  Ho  Er  Tm  Yb  Lu  Hf  Ta  W   Re  Os  Ir  Pt  Au  Hg  Tl  Pb  Bi  Po  At  Rn
Fr  Ra  Ac  Th  Pa  U   Np  Pu  Am  Cm  Bk  Cf  Es  Fm  Md  No  Lr  Rf  Db  Sg  Bh  Hs  Mt  Ds  Rg  Cn  Nh  Fl  Mc  Lv  Ts  Og
"""
ANGSTROM_TO_BOHR=1./0.52917721067

print_flush = partial(print, flush=True)

def atomic_charge_to_atom_type(Z):
    """ Return atom type when given an atomic Number """
    atom_types=PERIODIC_TABLE_STR.split()
    Z_to_atty_di=dict([ (i+1, at_ty) for i, at_ty in enumerate(atom_types)])
    assert Z in Z_to_atty_di.keys()
    return Z_to_atty_di[Z]

def switch_script(record):
    """ Run either my our the original entry calculation script"""
    method=record['method']
    if method.upper()=='PBE0-GRAC':
        return 'bruno'
    else:
        return 'original'

def psi4_copy(file, target_dir=None):
    if not isinstance(file, str):
        print(f"You did not provide string: {file}")
    elif not os.path.isfile(file):
        print(f"Requested to copy {file}, but is not a file.")
    elif not isinstance(target_dir, type(None)):
        print(f"Copying {file} to {target_dir}")
        if not os.path.dirname(os.path.realpath(file))==os.path.realpath(target_dir):
            try:
                p=subprocess.Popen(f"cp {file} {target_dir}", shell=True)
                p.communicate()
            except Exception as ex:
                print(f"Problem when trying to copy file: {ex}")

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
            assert len(timing_lines)==6, f"{timing_lines}, {output_file}"
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
    }
    for tag in ['psi4inp_file', 'psi4out_file', 'fchk_grac_file',  'fchk_neut_file']:#,'wfn_grac_file', 'wfn_neut_file']:
        # Get the associated keys
        assert tag in psi4_dict.keys() and zip_switch.keys()
        file=psi4_dict[tag]

        if tag in [ 'wfn_grac_file', 'wfn_neut_file' ]:
            p=subprocess.Popen(f"rm {file}", shell=True)
            p.communicate()
        else:
            if not isinstance(file, type(None)):
                requested_encodings=zip_switch[tag]
                tag=tag.replace('_file','')

                for encoding in requested_encodings:
                    dic=compress_file(file, encoding=encoding)
                    info_dic['size'].update(dict([
                        (f"{tag}_{k}",v) for k,v in dic.items()
                    ]))
                    # Copy the file to results dir
                    new_file=dic[encoding]['file_name']
                    assert isinstance(new_file,str), f"file {new_file} not valid"
                    if not isinstance(target_dir, type(None)):
                        psi4_copy(new_file, target_dir=target_dir)
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

    return info_dic

def run_psi4(atom_types, coordinates, dft_functional, basis_set,
            total_charge=0, multiplicity=1,
            jobname='test', hardware_settings=None, do_GRAC=False,
            target_dir=None):
    """ Interface between this data and my generic run script """
    psi4_start_time=time.time()

    # Generate the input file
    psi4_dict=complete_calc(
        atom_types, coordinates, total_charge=total_charge, multiplicity=multiplicity,
        dft_functional=dft_functional, do_GRAC=do_GRAC, basis_set=basis_set, 
        jobname=jobname, units={'LENGTH':'ANGSTROM'}, hardware_settings=hardware_settings
    )  
    psi4_input_file=psi4_dict['psi4inp_file']

    # Run the process
    process = subprocess.Popen(
        f"conda run -n qcAPI      psi4 {psi4_input_file}",
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        shell=True,
    )
    stdout, stderr = process.communicate()
    error_code =process.returncode

    # Recover the data
    psi4_dict.update({
        'exit_ana': psi4_exit_ana(psi4_dict['psi4out_file']),
        'real_time':time.time()-psi4_start_time,
        'time_unit':'s',
        'error_code':error_code,
        'stdout':stdout.decode('utf-8'),
        'stderr': stderr.decode('utf-8')})


    return psi4_dict
    
def extract_psi4_info(wfn_file, method, basis):
    """ Interface with generic run script """
    psi4_dict=property_calc(wfn_file, method, basis)
    return psi4_dict


def compute_entry_bruno(record, workder_id, num_threads=1, maxiter=150, target_dir=None, do_test=False):
    """ Cal psi4 calculation """
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
        print_flush('\n'.join(lines))
    def process_conformation(record, do_test=False):
        # Conformation data
        conformation = record["conformation"]
        if do_test:
            coordinates=[[0,0,0],[0,0,1]]
            species=[1,1]
        else:
            coordinates = conformation["coordinates"]
            species = conformation["species"]
        atom_types=[ atomic_charge_to_atom_type(x) for x in species ]
        multiplicity=1
        total_charge=conformation['total_charge']
        assert 'total_charge' in conformation.keys(), f"At the moment the charge needs to be defined unambigious by providing it in conformation, found {conformation.keys()}"
        return {
            'atom_types':atom_types,
            'coordinates': coordinates,
            'total_charge': total_charge,
            'multiplicity': multiplicity
        }
        
    def process_method(record):
        """ Read the method tag """

        method=record['method']
        basis_set=record['basis']
        
        # Parse the method for GRAC key
        method_components=method.split('-')
        if len(method_components)==1:
            dft_functional=method
            do_GRAC=False
        elif len(method_components)==2:
            assert method_components[1].upper()=='GRAC'
            dft_functional=method_components[0]
            do_GRAC=True
        else: 
            raise Exception()
        
        return {
            'dft_functional':dft_functional,
            'do_GRAC': do_GRAC, 
            'basis_set':basis_set,
        }

        
    # create ID
    jobname=f"{record['id']}_wid-{workder_id}"
    my_sep('START psi4 calculation','+')

    # Get input information
    psi4_di={}
    # Get conformational details (some are processed), possibly do a test by using Hydrogen molecule
    conf_di =process_conformation(record, do_test=do_test)
    # Method to be run on conformation
    method_di=process_method(record)
    # Hardware settings
    hardware_settings=dict(
        num_threads=4,
        memory='8GB',
    )
    [ psi4_di.update(x) for x in [conf_di, method_di]]
    psi4_di.update({'hardware_settings':hardware_settings, 'jobname':jobname})
    mandatory_keys=[ 'atom_types', 'coordinates']
    optional_keys=['total_charge', 'multiplicity',
            'dft_functional', 'basis_set', 'do_GRAC', 'jobname', 'hardware_settings', 'target_dir'
    ]
    assert all([x in psi4_di.keys() for x in mandatory_keys]), f"{psi4_di.keys()} {mandatory_keys}"
    assert all([x in mandatory_keys+optional_keys for x in psi4_di.keys()]), f"{psi4_di.keys()} {optional_keys+mandatory_keys}"

    def execute_psi4_job(target_dir):
        def make_dir(base_dir):
            # Make a directory (designated by job name) to run the changes within
            if isinstance(base_dir,type(None)):
                base_dir=os.getcwd()
            assert os.path.isdir(base_dir), f"The chosen target directory does not exist: {base_dir}"
            target_dir=os.path.join(base_dir,jobname)
            # over write if already exists
            if os.path.isdir(target_dir):
                print(f"Removing directory {target_dir} in order to create a new one")
                os.popen(f"rm -r {target_dir}")
            os.mkdir(target_dir)
            return target_dir
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
        # Make a target direcotry for the job (where things will be copied to)
        target_dir=make_dir(target_dir)

        # Get the wave function
        psi4_dict = run_psi4(**psi4_di)
        # In case the job fails clean manually and raise error
        if psi4_dict['error_code']!=0:
            # Delete files (in case of a failed job this is not done automatically)
            clean_psi4_manually()
            raise Exception(f"Psi4 wave function run did terminate with error: {psi4_dict['stderr']}")
        return psi4_dict, target_dir
    
    def drop_dictionary(jobname, target_dir, psi4_dict):
        try:
            info_json=jobname+'_ana.json'
            with open(info_json,'w') as wr:
                json.dump(psi4_dict, wr)
            if not isinstance(target_dir, type(None)):
                psi4_copy(info_json, target_dir=target_dir)
        except Exception as ex:
            print(f"Json file for performance analysis of job {jobname} could not be generated:\n{ex}")
    def drop_system_info(jobname, target_dir):
        try:
            system_json=jobname+'_sysinfo.json'
            system_dic=get_system_data()
            with open(system_json,'w') as wr:
                json.dump(system_dic, wr)
            if not isinstance(target_dir, type(None)):
                psi4_copy(system_json, target_dir=target_dir)
        except Exception as ex:
            print(f"Json file for system info of job {jobname} could not be generated:\n{ex}")
    def compress_target_dir(target_dir):
        try:
            import glob
            files=glob.glob(f"{target_dir}/*")
            for f in files:
                compres_di=[
                    {'ext':'.fchk', 'cmd':f"xz -v4 {f}", 'new_file':f"{f}.xz" }
                ]
                compressed=False
                for cd in compres_di:
                    assert not compressed, f"Double compression!"
                    extension=cd['ext']
                    new_file=cd['new_file']
                    cmd=cd['cmd']
                    if f.endswith(extension):
                        # Delete compressed file generate it and check if new file is there
                        if os.path.isfile(new_file):
                            os.remove(new_file)
                        p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)  # level four seeamed like a good compromise for systematic test on single file
                        out, err = p.communicate()
                        if not os.path.isfile(new_file):
                            print(f"Compression did not work: {cmd}\n{out}\n{err}")
                        compressed=True
        except Exception as ex:
            print(f"Could not compress results directory: {ex}")

        

    try:
        # Perpare directory
        psi4_dict, target_dir=execute_psi4_job(target_dir)

        # Check sizes, copy files
        run_analysis=psi4_after_run(psi4_dict,target_dir=target_dir, gzip=True, delete=True)
        psi4_dict.update(run_analysis)

        # Get derived information (atm this is not read in)
        # do_properties=False
        # if do_properties:
        #     output_conformation_add=extract_psi4_info(psi4_dict['wfn_grac_file'], dft_functional, basis_set)

        # Drop dictionary
        drop_dictionary(jobname, target_dir, psi4_dict)
        # Drop system info
        drop_system_info(jobname, target_dir)
        # Compress target directory
        compress_target_dir(target_dir)

        # If everything gone well return positive error code
        converged=1
        error=None

        my_sep('SUCCESS in  psi4 calculation','#')
    except Exception as ex:
        # In case of a test we want errors to kill the job so we notice them immediately
        converged=0
        error=str(ex)

        # Get hostname/ho
        hostname=os.popen("hostname").read().strip()
        my_sep('FAILURE in  psi4 calculation (on machine)','!',info_line=f"ERROR: {error}", sep_times=2)
        if do_test:
            raise Exception(f"Test was requested hence terminating:\n{ex}")
    
    
    ### RETURN the ouptu
    elapsed_time=time.time()-start_time
    # Immutable information
    output=dict(
        basis=record['basis'],
        method=record['method'],
        id=record.get("id", None),
        conformation=record['conformation'],
    )
    # Variable information, consider that this has to be the same otherwise the created unique identifier will be different
    output.update(dict(
        elapsed_time=elapsed_time,
        converged=converged,
        error=error,
    ))
    return output
