#!/usr/bin/env python3

from server_executions.client_server import main

import os
from util.util import check_dir_exists, available_properties

if __name__ == "__main__":
    import argparse
    description="Populate a qcAPI dataserv_adr with jobs"
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
        '--property', type=str, choices=available_properties,
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
