#!/usr/bin/env python

from . import *

from qcp_global_utils.shell_processes.execution import run_shell_command
from qcp_global_utils.multi_processing.multi_processing import spawn_workers, job

from qcp_global_utils.pydantic.pydantic import file, pdtc_directory
from functools import partial

from pydantic import validate_call ;  val_call=validate_call(config=dict(arbitrary_types_allowed=True))

@val_call
def the_function(cmd, worker_id, worker_tag="local_worker", directory:pdtc_directory|None=None):
    stem=f"{worker_tag}_{worker_id}"
    if directory is None:
        stem=stem
    else:
        assert os.path.isdir(directory), f"{directory}"
        stem= os.path.join(directory, stem) 
    stdout_file=f"{stem}.out"
    stderr_file=f"{stem}.err"
    stdout, stderr= run_shell_command(cmd, stdout_file=stdout_file, stderr_file=stderr_file)

@validate_call
def main_core(address:str, config:file, target_dir:pdtc_directory, prop:str, num_processes:int =10, num_threads_per_process:int =1, method:str|None=None,
    do_test=False, job_name=None
):
    worker_directory=f"{prop}_worker_output"
    if os.path.isdir(worker_directory): run_shell_command(f"rm -r {worker_directory}")
    os.mkdir(worker_directory)

    if not method is None:
        method=f"--method {method}"
    else:
        method=''
    loc=os.path.dirname(os.path.realpath(__file__))
    cmd=(
            f"qcp_server.py client {address} --config {config} --delay 10" \
        +f" --num_threads {num_threads_per_process}"
        +f" --target_dir {target_dir}  --property {prop} {method}" + ('' if not do_test else ' --test')
    )
    jobs=[ job( function=partial(the_function,cmd, ), args=[ worker_idx] , kwargs={
        'worker_tag':'local_worker' if job_name is None else job_name,
        'directory':worker_directory}
        ) for worker_idx in range(num_processes)]
    spawn_workers(jobs, num_processes=num_processes)

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    default_address='0.0.0.0:8000'
    default_num_proc=10
    default_threads_per_proc=1

    from data_base.utils import names as prop_names
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
    add(
        '--test', '-t', action='store_true', help=f"Will raise error in case worker fails",
    )
    #
    args=par.parse_args(argv)
    num_processes=args.num_processes
    num_threads_per_proc=args.num_threads_per_process
    target_dir=args.target_dir
    address=args.address
    prop=args.property
    method=args.method
    config=args.config
    do_test=args.test

    main_core(address, config, target_dir, prop, num_processes=num_processes, num_threads_per_process=num_threads_per_proc, method=method, do_test=do_test)
        

if __name__=='__main__':
    main()


