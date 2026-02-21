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
    ret=run_shell_command(cmd, stdout_file=stdout_file, stderr_file=stderr_file)

@validate_call
def main_core(config_file:pdtc_file, target_dir:pdtc_directory, prop:str,
        num_processes:int =10, 
        num_threads_per_process:int =1, 
        memory_gb_per_process:float|None=None,
        method:str|None=None, do_test=False, job_name=None
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
            f"qcp_server.py client --config {config_file}  --delay 10" \
        +f" --num_threads {num_threads_per_process}"
        +(f" --memory {memory_gb_per_process}" if not memory_gb_per_process is None else '' )
        +f" --target_dir {target_dir}  --property {prop} {method}" + ('' if not do_test else ' --test')
    )
    jobs=[ job( function=partial(the_function,cmd, ), args=[ worker_idx] , kwargs={
        'worker_tag':'local_worker' if job_name is None else job_name,
        'directory':worker_directory}
        ) for worker_idx in range(num_processes)]
    spawn_workers(jobs, num_processes=num_processes)

from .wrapper import process_argv, add_client_args,add_property_arg, get_property_args,process_client_args
from argparse import ArgumentParser, Namespace

DEFAULT_NUM_PROC=10
DEFAULT_THREADS_PER_PROC=1
@process_argv
def main(argv:list|Namespace):
    description=f"Spawn multiple workers through calling qcp_server.py client ..."
    prog=None
    #prog=f"{__file__} {default_address} --num_processes --property wfn"
    epilog=None
    par=ArgumentParser(description=description, prog=prog, epilog=epilog)


    par=add_client_args(par, require_config=True)
    par=add_property_arg(par)

    add=par.add_argument
    add(
        '--num_processes', '--np', default=DEFAULT_NUM_PROC, help=f"Number of process"
    )
    add(
        '--num_threads_per_process', '--nt', default=DEFAULT_THREADS_PER_PROC, help=f"How many threads should each process be run with"
    )
    add(
        '--memory_per_process', '--mem', type=float, default=None, help=f"Memory (in GB) to allocate for each process"
    )
    add(
        '--target_dir','--trgt', required=True, help=f"Directory where to store results"
    )
    add(
        '--method', '-m', help=f"Which method to select",
    )
    add(
        '--test', '-t', action='store_true', help=f"Will raise error in case worker fails",
    )
    #
    args=par.parse_args(argv)
    address, config_file=process_client_args(args, require_config=True)
    prop=get_property_args(args)
    #
    num_processes=args.num_processes
    num_threads_per_proc=args.num_threads_per_process
    memory_gb=args.memory_per_process
    target_dir=args.target_dir
    method=args.method
    do_test=args.test

    main_core(config_file, target_dir, prop, 
              num_processes=num_processes, 
              num_threads_per_process=num_threads_per_proc, 
              memory_gb_per_process=memory_gb,
              method=method, do_test=do_test)
        

if __name__=='__main__':
    main()


