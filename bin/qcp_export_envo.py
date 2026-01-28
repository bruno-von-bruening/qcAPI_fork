#!/usr/bin/env python

# Need to have this package installed! (side project from qcp_global_utils project)
from qcp_versioning.package_docu import main as main_pkg_docu

def main(*args, **kwargs):
    main_pkg_docu(*args, **kwargs)


MAIN_PKG='qcpAPI'
DEPENDENCIES=[
    'qcp_orm',
    'qcp_objects',
    'qcp_global_utils',
    'run_camcasp',
    'run_psi4',
]

if __name__ == '__main__':
    import argparse as ap
    desc="Export the conda environment with the current package versions." \
    "If nothing provided then just print a yaml file informing about the current environment." \
    "If a conda environment file is provided, then update it with the current package versions." 
    epilog='\n'.join(["Example usage:"]+[ f"  - {x} : {y}" for x,y in [
        ('Print current environment info to file', 'qcp_export_envo.py'),
        ('Update existing conda environment file', 'qcp_export_envo.py -f qcpAPI_env.yaml'),
        ('Test updated conda environment', 'qcp_export_envo.py -f qcpAPI_env.yaml -t'),
    ] ] +[
        f"Finally activate this environment and check if it passes the tests."
    ])
    par=ap.ArgumentParser(description=desc, epilog=epilog, formatter_class=ap.RawDescriptionHelpFormatter)
    add=par.add_argument
    add( '--env-file', '-f', type=str, default=None,
        help="Path to the conda environment file. It will be update with current environment data (can be distable through --no-update)."
    )
    # give option to leave conda file alond and just test it
    add( '--no-update', '-n', action='store_true',
        help="If set, then do not update the conda environment file. but just test it"
    )
    add( '--test', '-t', action='store_true',
        help="If set, then do not write the updated environment to file, just print it."
    )
    args=par.parse_args()
    conda_env_file=args.env_file
    test=args.test

    main(MAIN_PKG, DEPENDENCIES, conda_env_file=conda_env_file, test=test, no_update=args.no_update)
