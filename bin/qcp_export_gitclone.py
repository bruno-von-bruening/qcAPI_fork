#!/usr/bin/env python3

from qcp_versioning.package_docu import export_clone_script
from qcp_global_utils.environment.conda_env import get_conda_base
from qcp_global_utils.pydantic.pydantic import pdtc_file
# from qcp_global_utils.shell_processes.execution import run_shell_command
import os, yaml

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Export a git clone script for the qcpAPI repository")


    # check that it is file
    parser.add_argument('--environment_file','-f', type=pdtc_file, required=True, )
    #parser.add_argument('--envo_name', '-n', type=str, default='qcpAPI_env.yaml', help="Name of conda environment file to be found")

    args = parser.parse_args()
    envo_file=args.environment_file
    
    try:
        content=yaml.safe_load(open(envo_file))
    except Exception as e:
        raise ValueError(f"Error loading environment file {envo_file}: {e}")
    assert 'environment' in content, f"Missing 'environment' key in environment file {envo_file}"
    envo_info=content['environment']
    assert 'run_psi4' in envo_info, f"Missing 'run_psi4' key in environment file {envo_file}"
    run_psi4_info=envo_info['run_psi4']

    from util.config import env_entry
    try:
        env=env_entry(**run_psi4_info)
        script=env.script_fullpath
    except Exception as e:
        raise ValueError(f"Error parsing environment file {envo_file} into env_entry for '{run_psi4_info}': {e}")

    origin=os.getcwd()
    os.chdir(os.path.dirname(envo_file))
    cmd=f"{script} --version_info"
    try:
        # capture output
        import subprocess
        output=subprocess.run(cmd, capture_output=True, text=True, shell=True).stdout.strip()
        assert isinstance(output, str) and len(output)>0, f"Expected non-empty string output from command '{cmd}' but got: '{output}'"
    except Exception as e:
        raise Exception(f"Error running command '{cmd}': {e}")

    try:
        expected_form=r'[A-Za-z1-9_]+ g[0-9a-f]{9}'
        import re
        assert re.fullmatch(expected_form, output), f"Expected output to be a git commit hash of form {expected_form} but got: '{output}'"
        name,hash=output.split()
        hash=hash[1:] # remove leading 'g' if present
    except Exception as e:
        raise ValueError(f"Error validating output of command '{cmd}': {e}")

    from qcp_versioning.package_docu import export_clone_script
    fi=export_clone_script(name, hash, private=True, envo_name=name)
    print(f"Exported git clone script for environment in file {envo_file} with name '{name}' and hash '{hash}' to: {fi}")

    os.chdir(origin)


if __name__ == "__main__":
    main()


        
    