#!/usr/bin/env python3

from . import *
from server_executions.client.main import main as main_internal

from util.util import check_dir_exists
from data_base.utils import AVAILABLE_PROPERTIES

def main(argv):
    argv = argv if argv is not None else sys.argv[1:]

    description="Start worker that will communicate with central database until there are no jobs to distribute"
    prog=None
    epilog=None
    parser = argparse.ArgumentParser(description=description, prog=prog, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    adar=parser.add_argument
    adar(
        "address",
        type=str,
        default="127.0.0.1:8000",
        help="URL:PORT of the qcAPI server",
    )
    adar(
        "--num_threads", "-n", type=int, default=1, help="Number of threads to use"
    )
    adar(
        "--maxiter", "-m", type=int, default=150, help="Maximum number of SCF iterations"
    )
    adar(
        "--delay", "-d", type=float, default=60, help="Ping frequency in seconds"
    )
    adar(
        '--target_dir', type=str, help='where to save file', default=os.getcwd()
    )
    adar(
        '--test', action='store_true', help='run test using hydrogen molecule and excepting when error is encountered in psi4 run'
    )
    adar(
        '--do_lisa', action='store_true', help='run lisa job'
    )
    adar(
        '--fchk_link', type=str, help='file where fchk_files are stored',
    )
    adar(
        '--property', type=str, choices=AVAILABLE_PROPERTIES,
    )
    adar(
        '--method' , type=str
    )
    adar(
        '--basis',   type=str
    )
    adar(
        '--config', type=str, required=True
    )
    

    args = parser.parse_args(argv)
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    target_dir=args.target_dir
    do_test=args.test
    num_threads=args.num_threads
    max_iter=args.maxiter
    delay=args.delay
    config_file=os.path.realpath(args.config)

    # filter by property and specs
    property    = args.property
    method      = args.method
    basis       = args.basis

    check_dir_exists(target_dir)

    main_internal(config_file,url, port, num_threads, max_iter, delay, target_dir=target_dir, do_test=do_test, property=property, method=method)
if __name__ == "__main__":
    main()
