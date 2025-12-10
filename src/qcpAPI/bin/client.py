#!/usr/bin/env python3

from . import *
from server_external.client.main import main as main_internal

from util.util import check_dir_exists
from .wrapper import add_client_args, process_client_args, add_property_arg, get_property_args
from orm_import.utils import AVAILABLE_PROPERTIES

def main(argv):
    argv = argv if argv is not None else sys.argv[1:]

    description="Start worker that will communicate with central database until there are no jobs to distribute"
    prog=None
    epilog=None
    parser = argparse.ArgumentParser(description=description, prog=prog, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser=add_client_args(parser, require_config=True)
    parser=add_property_arg(parser)
    adar=parser.add_argument

    adar(
        "--num_threads", "-n", type=int, default=1, help="Number of threads to use"
    )
    adar(
        f"--memory", "-mem", type=float, default=4, help="Memory (in GB) to allocate for each thread"
    )
    adar(
        '--target_dir', type=str, help='where to save file', default=os.getcwd()
    )
    adar(
        '--test', action='store_true', help='Raise terminating error if error encountered during production'
    )

    # Somewhat legacy
    adar(
        "--maxiter", "-m", type=int, default=150, help="Maximum number of SCF iterations"
    )
    adar(
        "--delay", "-d", type=float, default=60, help="Ping frequency in seconds"
    )
    adar(
        '--do_lisa', action='store_true', help='run lisa job'
    )
    adar(
        '--fchk_link', type=str, help='file where fchk_files are stored',
    )
    adar(
        '--method' , type=str
    )
    adar(
        '--basis',   type=str
    )
    
    args = parser.parse_args(argv)
    address, config_file=process_client_args(args, require_config=True)
    property=get_property_args(args)

    target_dir=args.target_dir
    do_test=args.test
    num_threads=args.num_threads
    memory=args.memory
    max_iter=args.maxiter
    delay=args.delay

    # filter by property and specs
    method      = args.method
    basis       = args.basis

    check_dir_exists(target_dir)

    main_internal(
        config_file, 
        num_threads, memory,
        max_iter, delay, target_dir=target_dir, do_test=do_test, property=property, method=method)
if __name__ == "__main__":
    main()
