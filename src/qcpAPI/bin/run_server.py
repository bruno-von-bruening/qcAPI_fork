#!/usr/bin/env python

from . import *
from server_executions.launch_server import main as main_internal

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

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
    args=par.parse_args(argv)
    config_file=args.config
    host=args.host
    port=args.port


    assert os.path.isfile(config_file), f"Not a file {config_file}"

    main_internal(config_file, host, port)

if __name__=='__main__':
    main()



