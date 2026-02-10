#!/bin/env python

from qcp_global_utils.shell_processes.execution import run_shell_command
from qcp_global_utils.environment.file_handling import load_json_or_yaml

import os,sys
HOME=os.path.realpath('../../../')

# add the $HOME/bin/qcp_export_env.py file to path

def main(conda_env_file=None,install=True):

    if not conda_env_file:
        conda_env_file=os.path.join(HOME, 'install', 'qcpAPI_env.yaml')

    sys.path.insert(1,os.path.join(HOME, 'bin'))
    from qcp_export_envo import MAIN_PKG, DEPENDENCIES
    from qcp_versioning.package_docu import main as main_install


    # First export the environment and install it (see if conda env file is correct)
    if install:
        new_envo_file=main_install(   MAIN_PKG, DEPENDENCIES, conda_env_file=conda_env_file, test=False)
        new_envo_data=load_json_or_yaml(new_envo_file)
        name=new_envo_data['name']
        print(f"Removing old environment {name}")
        run_shell_command(f"conda env remove -n {name} -y")
    new_envo_file=main_install(   MAIN_PKG, DEPENDENCIES, conda_env_file=conda_env_file, test=install)
    if install:
        run_shell_command( f" conda run -n {name} python -m build {HOME}; conda run -n {name} pip install {HOME}" )

    new_envo_data=load_json_or_yaml(new_envo_file)
    name=new_envo_data['name']
    print(f"Exported environment {name} to {new_envo_file}"+("." if not install else f" (and installed it)."))

    # Activate the environment and run test
    origin=os.getcwd()
    try:
        test_dir=os.path.join(HOME, 'tests','molpol_run')
        os.chdir(test_dir)
        print(f"Will run environment {name} in test directory {test_dir}")
        run_shell_command( f"conda run -n {name} python test.py" )
    except Exception as ex:
        raise Exception(f"Error in running test script for the new environment. Error was: {str(ex)}") from ex
    finally:    os.chdir(origin)


if __name__ == '__main__':
    import argparse as ap
    parser=ap.ArgumentParser(description="Test the installation of the environment by running a test script in a test directory with the new environment.")
    parser.add_argument( '-n', '--no-install', action='store_true', help="Do not perform installation, only run the test with the already exported environment (e.g. for testing the test script or the exported environment file).")
    parser.add_argument( '-f', '--file', type=str, help="Path to the conda environment file to be exported and installed (if not provided, default is the install/qcpAPI_env.yaml file).")
    args=parser.parse_args()
    main(conda_env_file=args.file, install=not args.no_install)

# run_shell_command( f"qcp_export_env.py -f {install_file} -t" )



