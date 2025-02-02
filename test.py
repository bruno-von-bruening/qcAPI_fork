#!/usr/bin/env python

import os; from os.path import isfile, isdir
import subprocess
import time
import yaml
import copy


def is_running(process):
    """ The result of poll should be none if the process is running"""
    return isinstance(process.poll(), type(None))
def kill_process(process):
    """ Kill the process for some reason pid is one larger (maybe do to python counting convention)
    """
    os.system(f"kill {process.pid+1}")
    time.sleep(1)

def run_process(cmd, limit_time=False, time_limit=10, tag=None):
    """ """

    if isinstance( tag , type(None) ):
        tag_str=''
    else:
        tag_str=f"{tag}"
    err_file=f'{tag_str}run.err'
    out_file=f'{tag_str}run.out'
    new_cmd=f" {cmd}  1> {out_file} 2> {err_file}" # Copies to both file and terminal
    process=subprocess.Popen(new_cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, preexec_fn=os.setsid)
    print(f"RUNNING {tag}: (at {os.getcwd()})\n    {new_cmd}")

    if limit_time:
        # Limit time of process
        start=time.time()
        terminated=False
        while time.time()-start<time_limit:
            print(f"Checking time")
            if not is_running(process):
                terminated=True
                break
            time.sleep(1)
        if not terminated:
            print(f"Passed maximum time, terminating")
            kill_process(process)
        else:
            pass
    stdout, stderr=process.communicate()
    with open(err_file,'r') as err, open(out_file) as out:
        stderr=err.read()
        stdout=out.read()
    if process.returncode !=0: # 0 is good returncode
        raise Exception(f"{stderr} {stdout}")#, stdout)
    print('Process finished')
    return stdout, stderr


def start_server(config_file):
    """ Return server handle"""
    assert config_file, f"Not a file: {config_file}"
    try:
        #  fastapi='/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/fastapi'
        #  assert isfile(fastapi)
        #  server=subprocess.Popen([f"{fastapi}","run","server.py"], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        python='python' # '/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python'
        # assert isfile(python)
        server=subprocess.Popen(f"{python} server.py --config {config_file}", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        pid=server.pid

        time.sleep(3)
        if not isinstance(server.poll(), type(None)):
            kill_process(server)
            stdout, stderr = server.communicate()
            raise Exception(f"Exiting do to server shutdown! ({stderr})")
        else:
            print(f"Server started with process id: {pid}")
        
    except Exception as ex:
        raise Exception(f"Cannot open server: \n{ex}")
    return server


def make_dummy_config_file():
    config_file=f"test_config.yaml"
    config_content='\n'.join([
        f"database: database_test.db"
    ])
    with open(config_file, 'w') as wr:
        wr.write(config_content)
    return config_file
defaults=dict(
    qm_method='pbe0-grac',
    qm_basis='sto-3g',
)
def load_config(config_file):
    """ Read the provided file and fills in default values"""
    with open(config_file,'r') as rd:
        original_config=yaml.safe_load(rd)
    updated_config=copy.deepcopy(original_config)
    for key,value in defaults.items():
        if not key in original_config.keys():
            print(f"INFO: Key {key} not found in config keys ({list(original_config.keys())}:\n{' '*4}Resorting to default value {value}")
            updated_config.update({key:value})
    return updated_config


if __name__=="__main__":
    import argparse
    description=None
    epilog=None
    par=argparse.ArgumentParser(description=description, epilog=epilog)
    par.add_argument(
        '--config', type=str, help=f"Config file for this server", default='config_test.yaml'
        )
    args=par.parse_args()
    config_file=args.config
    if not os.path.isfile(config_file):
        config_file=make_dummy_config_file()
    config=load_config(config_file)
    

    test_dir='test/'
    target_dir='test_copy_files_target/'
    # Start the server
    address='127.0.0.1:8000'
    server=start_server(config_file)


    fl=dict(
        populate_wfn    =False,
        compute_wfn     =False,
        populate_lisa   =True,
        compute_lisa    =True,
        populate_mbis   =True,
        compute_mbis    =True,
    )
    fl=dict(
        populate_wfn    =True,
        compute_wfn     =True,
        populate_lisa   =False,
        compute_lisa    =False,
        populate_mbis   =False,
        compute_mbis    =False,
    )
    fl=dict(
        populate_wfn    =True,
        compute_wfn     =True,
        populate_lisa   =True,
        compute_lisa    =True,
        populate_mbis   =True,
        compute_mbis    =True,
    )
    try:
        qm_method=config['qm_method']
        qm_basis=config['qm_basis']
        # Work inside test dir and delete the dir in case it exists
        def make_dirs(test_dir, target_dir):
            def make_rm_dir(the_dir):
                if isdir(the_dir):
                    p=subprocess.Popen(f"rm {the_dir} -r ", shell=True)
                    x=p.wait()
                os.mkdir(the_dir)
            make_rm_dir(test_dir)
            os.chdir(test_dir)
            make_rm_dir(target_dir)
        make_dirs(test_dir, target_dir)


        python="python" #/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python"
        tag='populate_wfn'
        if fl[tag]:
            # Populate the database
            cmd=f"{python} ../populate_db.py --filenames ../test_sample.pkl --address 127.0.0.1:8000 --property wfn --method={qm_method} --basis={qm_basis} --test"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=20, tag=tag)

        tag='compute_wfn'
        if fl[tag]:
            # Run the wfn calculation
            python=python # same as before
            cmd=f"{python} ../client.py {address} --num_threads 4 --target_dir {target_dir} --test --property wfn"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=20, tag=tag)

        tag='populate_lisa'
        if fl[tag]:
            python=python # qcapi python
            #fchk_link_file="/home/bruno/1_PhD/2-2_Software/qcAPI_expand_db/test_copy_files_target/transfer_fchks/meta_info.json"
            cmd=f"{python} ../populate_db.py --address 127.0.0.1:8000 --property part --method LISA"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=5, tag=tag)

        tag='compute_lisa'
        if fl[tag]:
            python="/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python"
            cmd=f"{python} ../client.py {address} --property part --method LISA --target_dir {target_dir}"
            stdout, stderr = run_process(cmd, limit_time=True, time_limit=10, tag=tag)
        
        tag='populate_mbis'
        if fl[tag]:
            python=python # qcapi python
            #fchk_link_file="/home/bruno/1_PhD/2-2_Software/qcAPI_expand_db/test_copy_files_target/transfer_fchks/meta_info.json"
            cmd=f"{python} ../populate_db.py --address 127.0.0.1:8000 --property part --method MBIS"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=20, tag=tag)

        tag='compute_mbis'
        if fl[tag]:
            python="/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python"
            cmd=f"{python} ../client.py {address} --property part --method MBIS --target_dir {target_dir}"
            stdout, stderr = run_process(cmd, limit_time=True, time_limit=10, tag=tag)
        
        kill_process(server)
    except Exception as ex:
        kill_process(server)
        raise Exception(f"Script terminated with error (Killing Server): {ex}")



