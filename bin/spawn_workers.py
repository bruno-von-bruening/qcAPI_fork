#!/usr/bin/env python

from qc_global_utilities.shell_processes.execution import run_shell_command
from qc_global_utilities.multi_processing.multi_processing import spawn_workers

from qc_global_utilities.pydantic.pydantic import file, directory
from functools import partial

from pydantic import validate_call
import os

def the_function(cmd, worker_id ):
    stem=f"local_worker_{worker_id}"
    stdout_file=f"{stem}.out"
    stderr_file=f"{stem}.err"
    stdout, stderr= run_shell_command(cmd, stdout_file=stdout_file, stderr_file=stderr_file)

@validate_call
def main(address:str, config:file, target_dir:directory, prop:str, num_processes:int =10, num_threads_per_process:int =1, method:str|None=None):
    if not method is None:
        method=f"--method {method}"
    else:
        method=''
    loc=os.path.dirname(os.path.realpath(__file__))
    cmd=(
            f"{loc}/client.py {address} --config {config} --delay 10" \
        +f" --num_threads {num_threads_per_proc}"
        +f" --target_dir {target_dir}  --property {prop} {method}"
    )
    jobs=[ dict( function=partial(the_function,cmd ), args=worker_idx, kwargs={} ) for worker_idx in range(num_processes)]
    spawn_workers(jobs, num_processes=num_processes)
        

if __name__=='__main__':
    default_address='0.0.0.0:8000'
    default_num_proc=10
    default_threads_per_proc=1

    from util.util import names as prop_names
    prop_choices=list( prop_names.keys() )

    description=None
    prog=f"{__file__} {default_address} --num_processes --property wfn"
    epilog=None
    import argparse; par=argparse.ArgumentParser(description=description, prog=prog, epilog=epilog)
    add=par.add_argument
    add(
        '--num_processes', '--np', default=default_num_proc, help=f"Number of process"
    )
    add(
        '--num_threads_per_process', '--nt', default=default_threads_per_proc, help=f"How many threads should each process be run with"
    )
    add(
        '--target_dir','--trgt', required=True, help=f"Directory where to store results"
    )
    add(
        '--address', required=False, default=default_address, help=f"Http address where database server runs"
    )
    add(
        '--property', '-p', required=True, help=f"Which property to run", choices=prop_choices,
    )
    add(
        '--method', '-m', help=f"Which method to select",
    )
    add(
        '--config', '-c', required=True, help=f"Config file to provide",
    )
    #
    args=par.parse_args()
    num_processes=args.num_processes
    num_threads_per_proc=args.num_threads_per_process
    target_dir=args.target_dir
    address=args.address
    prop=args.property
    method=args.method
    config=args.config

    main(address, config, target_dir, prop, num_processes=num_processes, num_threads_per_process=num_threads_per_proc, method=method)
