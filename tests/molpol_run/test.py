#!/usr/bin/env python

from test_helper import start_server
import os, shutil
import yaml
from subprocess import call
EDITOR = os.environ.get('EDITOR', 'vim')
def edit(fi):
    call([EDITOR,fi])

from server_external.launch_server import make_auto_config_file
from qcp_global_utils.shell_processes.execution import run_shell_command

config_file="config.yaml"
if not os.path.isfile(config_file):
    from util.config import qcAPI_server_config,qcAPI_storage_info


    auto_config=make_auto_config_file(host='localhost', port=8101)
    shutil.move(auto_config,config_file)
    conf=yaml.safe_load(open(config_file))
    conf['database_file']='test_molpol.db'
    conf=qcAPI_server_config(**conf, source=os.path.realpath(config_file)).model_dump(exclude=['source','environment','TAG'])

    with open(config_file,"w") as f:
        yaml.safe_dump(conf,f)
    edit(config_file)
    print(f"Dropped automatic config under {config_file}")

if not os.path.isdir('scratch'): os.mkdir('scratch')
if not os.path.isdir('storage'): os.mkdir('storage')

conf=yaml.safe_load(open(config_file))
db_file=conf['database_file']
if os.path.isfile(db_file):
    os.remove(db_file)
    print(f"Removed old database file: {db_file}")
run_shell_command(f"rm -r scratch/* || true")

proc=start_server(config_file)

conf_file='../supplementary_files/conformations/h2.yaml'
assert os.path.isfile(conf_file), f"Not a file: {conf_file}"

lots_file_small='../supplementary_files/lots/lots_dummy_wfn_set.yaml'
assert os.path.isfile(lots_file_small), f"Not a file: {lots_file_small}"

lots_wider_file='../supplementary_files/lots/lots_wider_sampling.yaml'
assert os.path.isfile(lots_wider_file), f"Not a file: {lots_wider_file}"

lots_file=lots_wider_file

molpol_file='../supplementary_files/molpol/molpol.yaml'
assert os.path.isfile(molpol_file), f"Not a file: {molpol_file}"

def run_wrapper(cmd):
    print(f"Running command: {cmd}")
    stderr, stdout=run_shell_command(cmd)
    print(f"Finished command successfully. STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
try:
    lead_command=f"python {shutil.which('qcp_server.py')}"

    # Populate database
    cmd=f"{lead_command} populate --config {config_file} -p Conformation --files {conf_file}"
    run_wrapper(cmd)

    cmd=f"{lead_command} populate --config {config_file} -p Wave_Function --files {lots_file}"
    run_wrapper(cmd)

    cmd=f"{lead_command} populate --config {config_file} --p Molecular_Polarizability --files {molpol_file}" 
    run_wrapper(cmd)

    cmd=f"{lead_command} client --config {config_file} --p Molecular_Polarizability --target scratch/ --test"
    run_wrapper(cmd)

finally:
    proc.terminate()
    proc.wait()
