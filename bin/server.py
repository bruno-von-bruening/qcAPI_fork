#!/usr/bin/env python
import sys, os
sys.path.insert(1, os.path.realpath('../src'))
from server_executions.launch_server import main
if __name__=='__main__':

    import argparse, os
    description=None
    epilog=None
    prog=None
    par=argparse.ArgumentParser(prog=prog, description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter,)
    adar=par.add_argument
    adar(
        '--config', type=str, help=f"Config file in yaml format", required=True
        )
    adar(
        '--host', type=str, help=f"IP of host", default='0.0.0.0'
        )
    adar(
        '--port', type=int, help=f"Port", default=8000
        )
    # Parse arguments
    args=par.parse_args()
    config_file=args.config
    host=args.host
    port=args.port


    assert os.path.isfile(config_file), f"Not a file {config_file}"

    main(config_file, host, port)


