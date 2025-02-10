#!/usr/bin/env python3

from server_executions.client_server import main

import os
from util.util import check_dir_exists

if __name__ == "__main__":
    import argparse
    description="Populate a qcAPI dataserv_adr with jobs"
    prog=None
    epilog=None
    parser = argparse.ArgumentParser(description=description, prog=prog, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "address",
        type=str,
        default="127.0.0.1:8000",
        help="URL:PORT of the qcAPI server",
    )
    parser.add_argument(
        "--num_threads", "-n", type=int, default=1, help="Number of threads to use"
    )
    parser.add_argument(
        "--maxiter", "-m", type=int, default=150, help="Maximum number of SCF iterations"
    )
    parser.add_argument(
        "--delay", "-d", type=float, default=60, help="Ping frequency in seconds"
    )
    parser.add_argument(
        '--target_dir', type=str, help='where to save file', default=os.getcwd()
    )
    parser.add_argument(
        '--test', action='store_true', help='run test using hydrogen molecule and excepting when error is encountered in psi4 run'
    )
    parser.add_argument(
        '--do_lisa', action='store_true', help='run lisa job'
    )
    parser.add_argument(
        '--fchk_link', type=str, help='file where fchk_files are stored',
    )
    parser.add_argument(
        '--property', type=str
    )
    parser.add_argument(
        '--method' , type=str
    )
    parser.add_argument(
        '--basis',   type=str
    )
    parser.add_argument(
        '--config', type=str,
    )
    

    args = parser.parse_args()
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

    main(config_file,url, port, num_threads, max_iter, delay, target_dir=target_dir, do_test=do_test, property=property, method=method)
