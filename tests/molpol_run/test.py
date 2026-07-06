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
from util.config import qcAPI_server_config,qcAPI_storage_info

from typing import List

QCPAPI_HOME=os.path.join( os.path.dirname(os.path.realpath(__file__)), '..', '..' )
assert os.path.isdir(QCPAPI_HOME), f"Not a directory: {QCPAPI_HOME}"
os.environ['QCPAPI_HOME']=os.path.realpath(QCPAPI_HOME)

test_dir=os.path.join(os.environ['QCPAPI_HOME'],'tests')
assert os.path.isdir(test_dir), f"Not a directory: {test_dir}"

config_file_default='config.yaml'

def gen_auto_config_file(config_file=config_file_default, edit=True):
    auto_config=make_auto_config_file(host='localhost', port=8101)
    shutil.move(auto_config,config_file)
    conf=yaml.safe_load(open(config_file))
    conf['database_file']=f"test_molpol.db"
    yaml.safe_dump(conf, open(auto_config,'w'))
    if edit:
        edit(auto_config)

    conf=yaml.safe_load(open(auto_config))
    conf=qcAPI_server_config(**conf, source=os.path.realpath(config_file)).model_dump(exclude=['source','environment','TAG'])

    with open(config_file,"w") as f:
        yaml.safe_dump(conf,f)
    print(f"Dropped automatic config under {config_file}")
    return config_file


def run_wrapper(cmd):
    def break_text(lines:List[str]):
        import textwrap
        import shutil
        tag=' | '
        width = shutil.get_terminal_size().columns -len(tag)
        text='\n'.join([
            textwrap.fill( l.strip('\n'), width=width)
            for l in lines
        ]).split('\n')
        return '\n'.join([ tag+x for x in text ])

    print(f"Running command: {cmd}")
    ret=run_shell_command(cmd, uncritical=True)
    stdout,stderr=[ ret[x] for x in ['stdout','stderr'] ]
    out=f"STDOUT:\n{break_text(stdout)}"
    out+=f"\nSTDERR:" + (f"\n{break_text(stderr)}" if len(stderr)>0 else f" Nothing on record" )
    if ret['returncode']!=0:
        raise Exception(f"Command '{cmd}' failed with return code {ret['returncode']}. Output:\n{out}")
    else:
        print(f"Command '{cmd}' executed successfully. Output:\n{out}")

def main(config_file):
    conf_file=os.path.join(test_dir,'supplementary_files','conformations','h2.yaml')
    assert os.path.isfile(conf_file), f"Not a file: {conf_file}"

    lots_file_small=os.path.join(test_dir,'supplementary_files','lots','lots_dummy_wfn_set.yaml')
    assert os.path.isfile(lots_file_small), f"Not a file: {lots_file_small}"

    lots_wider_file=os.path.join(test_dir,'supplementary_files','lots','lots_wider_sampling.yaml')
    assert os.path.isfile(lots_wider_file), f"Not a file: {lots_wider_file}"

    lots_cc_hyb_df_file='../supplementary_files/lots/cc_hyb-bs_df.yaml'
    assert os.path.isfile(lots_wider_file), f"Not a file: {lots_cc_hyb_df_file}"

    lots_file=lots_wider_file
    lots_file=lots_cc_hyb_df_file

    molpol_file=os.path.join(test_dir,'supplementary_files','molpol','molpol_ff.yaml')

    assert os.path.isfile(molpol_file), f"Not a file: {molpol_file}"
    try:
        proc=start_server(config_file)
    except Exception as ex:
        raise Exception(f"Could not start server (with config file \'{config_file}\')") from ex
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

if __name__=="__main__":
    import argparse as ap
    parser=ap.ArgumentParser(description="Test the molpol run")
    parser.add_argument('--config', '-c', type=str, default=config_file_default, help="Path to config file. If not provided, will be generated automatically under config.yaml")
    parser.add_argument('--edit', '-e', action='store_true', help="Edit the config file after generation", default=False)
    args=parser.parse_args()
    if args.edit:
        edit(args.config)

    if not os.path.isfile(args.config):
        print(f"Config file {args.config} not found. Generating automatically.")
        gen_auto_config_file(edit=args.edit)
    else:
        print(f"Using provided config file: {args.config}")

    if not os.path.isdir('scratch'): os.mkdir('scratch')
    if not os.path.isdir('storage'): os.mkdir('storage')

    conf=yaml.safe_load(open(args.config))
    db_file=conf['database_file']
    if os.path.isfile(db_file):
        os.remove(db_file)
        print(f"Removed old database file: {db_file}")
    run_shell_command(f"rm -r scratch/* -f || true")

    main(args.config)
