#!/usr/bin/env python
import os,sys; from os.path import isfile, isdir
import subprocess as sp; import signal
import time
import yaml
import copy

# paths
def make_file(name, path):
    new_file=os.path.realpath(os.path.join(path, name))
    assert os.path.isfile(new_file), f"Not a file \'{new_file}\'"
    return new_file
qcapi_home=os.path.realpath('../../')
assert os.path.isdir(qcapi_home)

# scripts
qcapi_bin=os.path.realpath(os.path.join(qcapi_home,'bin'))
assert os.path.isdir(qcapi_bin)
server_file=make_file('server.py',qcapi_bin)
populate_script=make_file('populate_db.py',qcapi_bin)
client_script=make_file('client.py', qcapi_bin)
# data
geom_file=make_file('tests/supplementary_files/test_sample.pkl', qcapi_home)

def is_running(process):
    """ The result of poll should be none if the process is running"""
    return isinstance(process.poll(), type(None))
def kill_process(process):
    """ Kill the process for some reason pid is one larger (maybe do to python counting convention)
    """
    # Do not terminate with process.pid for some reason the pid is different between two version I have tried (maybe that is about the library version or the Linux version)
    pid= process.pid
    try:
        # Kill the process group
        p=sp.Popen(f"kill -- {pid}", shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        stdout, stderr = p.communicate()
        
        if p.returncode!=0:
            raise Exception(stderr.decode())
        time.sleep(1)

        stderr, stdout=process.communicate()
        return stderr.decode(), stdout.decode()
    except Exception as ex:
        raise Exception(f"Could not kill server with {pid}: {str(ex)}")
        print(f"Could not kill process  with pid {pid} : {str(ex)}")

def run_process(cmd, limit_time=False, time_limit=10, tag=None):
    """ """

    if isinstance( tag , type(None) ):
        tag_str=''
    else:
        tag_str=f"{tag}"
    err_file=f'{tag_str}run.err'
    out_file=f'{tag_str}run.out'
    new_cmd=f" {cmd}  1> {out_file} 2> {err_file}" # Copies to both file and terminal
    process=sp.Popen(new_cmd, shell=True, stderr=sp.PIPE, stdout=sp.PIPE)#, preexec_fn=os.setsid)
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
    indent=4*' '
    stdout_indent='\n'.join([ indent+x for x in stdout.split('\n')])
    stderr_indent='\n'.join([ indent+x for x in stderr.split('\n')])
    if process.returncode != 0: # 0 is good returncode
        raise Exception(f"Process did not terminate normall:\nstderr=\n{stderr_indent}\nstdout=\n{stdout_indent}")#, stdout)
    print('Process finished')
    return stdout, stderr


def start_server(config_file, host, port):
    """ Return server handle"""
    assert config_file, f"Not a file: {config_file}"
    try:
        #  fastapi='/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/fastapi'
        #  assert isfile(fastapi)
        #  server=sp.Popen([f"{fastapi}","run","server.py"], stderr=sp.PIPE, stdout=sp.PIPE)
        python='python' # '/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python'
        # assert isfile(python)
        server=sp.Popen(f"{python} {server_file} --config {config_file} --host {host} --port {port}".split(), stderr=sp.PIPE, stdout=sp.PIPE)
        pid=server.pid

        time.sleep(3)
        if not isinstance(server.poll(), type(None)):
            #kill_process(server)
            stdout, stderr = server.communicate()
            error_with_indent='\n'.join([4*' '+x for x in stderr.decode().split('\n')])
            raise Exception(f"Exiting do to server shutdown! ERROR:\n{error_with_indent}")
        else:
            print(f"Server started with process id: {pid}")
        
    except Exception as ex:
        raise Exception(f"Cannot open server: \n{ex}")
    return server, f"{host}:{port}"


def make_dummy_config_file():
    config_file=f"test_config.yaml"
    psi4_script=f"{os.environ['SCR']}/execution/psi4_scripts/run_psi4_2.py"
    assert os.path.isfile(psi4_script)
    config_content='\n'.join([
        f"database: database_test.db",
        f"global:",
        #f"  psi4_script: /home/bvbruening/1-1_scripts/multipole_scripts/execution/psi4_scripts/run_psi4_2.py",
        f"  psi4_script: {psi4_script}",
    ])+'\n'
    with open(config_file, 'w') as wr:
        wr.write(config_content)
    return config_file
defaults=dict(
    #qm_method='pbe0-grac',
    qm_method='hf',
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

def run_test(config_file, host, port, qm_method, qm_basis, target_dir):
    # Start the server (return subprocess object)
    server, address=start_server(config_file,host, port)

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
    fl=dict(
        populate_wfn    =True,
        compute_wfn     =True,
        populate_lisa   =True,
        compute_lisa    =True,
        populate_mbis   =False,
        compute_mbis    =False,
    )
    fl=dict(
        populate_wfn    =True,
        compute_wfn     =True,
        populate_lisa   =True,
        compute_lisa    =True,
        populate_mbis   =False,
        compute_mbis    =False,
    )
    try:
        # Work inside test dir and delete the dir in case it exists
        def make_dirs(test_dir, target_dir):
            def make_rm_dir(the_dir):
                if isdir(the_dir):
                    p=sp.Popen(f"rm {the_dir} -r ", shell=True)
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
            cmd=f"{python} {populate_script} --filenames {geom_file} --address {address} --property wfn --method={qm_method} --basis={qm_basis} --test"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=10, tag=tag)

        tag='compute_wfn'
        if fl[tag]:
            # Run the wfn calculation
            python=python # same as before
            cmd=f"{python} {client_script} {address} --num_threads 4 --target_dir {target_dir} --property wfn --config {config_file} --test"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=10, tag=tag)

        tag='populate_lisa'
        if fl[tag]:
            python=python # qcapi python
            #fchk_link_file="/home/bruno/1_PhD/2-2_Software/qcAPI_expand_db/test_copy_files_target/transfer_fchks/meta_info.json"
            cmd=f"{python} {populate_script} --address 127.0.0.1:8000 --property part --method LISA"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=5, tag=tag)

        tag='compute_lisa'
        if fl[tag]:
            # python="/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python"
            python=python
            cmd=f"{python} {client_script} {address} --property part --method LISA --target_dir {target_dir} --config {config_file} --test"
            stdout, stderr = run_process(cmd, limit_time=True, time_limit=10, tag=tag)
        
        tag='populate_mbis'
        if fl[tag]:
            python=python # qcapi python
            #fchk_link_file="/home/bruno/1_PhD/2-2_Software/qcAPI_expand_db/test_copy_files_target/transfer_fchks/meta_info.json"
            cmd=f"{python} {populate_script} --address 127.0.0.1:8000 --property part --method MBIS"
            stdout, stderr=run_process(cmd, limit_time=True, time_limit=20, tag=tag)

        tag='compute_mbis'
        if fl[tag]:
            # python="/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python"
            python=python
            cmd=f"{python} {client_script} {address} --property part --method MBIS --target_dir {target_dir}"
            stdout, stderr = run_process(cmd, limit_time=True, time_limit=10, tag=tag)
        
        kill_process(server)
    except Exception as ex:
        stdout, stderr=kill_process(server)
        with open(f"server.out", 'w') as wr:
            wr.write(stdout)
        with open(f"server.err", 'w') as wr:
            wr.write(stderr)
        raise Exception(f"Script terminated with error (Killing Server): {ex}")

if __name__=="__main__":

    # Hardcoded parameters
    test_dir='test_full_run/'
    target_dir='test_copy_files_target/'
    #address='127.0.0.1:8000'
    host='0.0.0.0'
    port=8000

    # Define argparse
    import argparse
    description=f"Test the routines for qcAPI data production on small example (3 smallest entry from pickle file)"
    epilog=f"Usage example:\n    {__file__} --config config_test.yaml"
    par=argparse.ArgumentParser(description=description, epilog=epilog)
    # Add argumnets
    adar=par.add_argument
    adar(
        '--config', type=str, help=f"Config file for this server", default='config_test.yaml'
        )
    # parse arguments
    args=par.parse_args()
    config_file=args.config

    # process input
    if not os.path.isfile(config_file):
        config_file=make_dummy_config_file()
    config_file=os.path.realpath(config_file)
    config=load_config(config_file)
    qm_method=config['qm_method']
    qm_basis=config['qm_basis']
    
    run_test(config_file, host, port, qm_method, qm_basis, target_dir,)



