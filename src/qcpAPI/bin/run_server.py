#!/usr/bin/env python

from . import *
from server_external.launch_server import main as main_internal, make_auto_config_file, DEFAULT_CONFIG_FILE

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    description=None
    epilog=None
    prog=None
    par=argparse.ArgumentParser(prog=prog, description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter,)
    adar=par.add_argument
    adar(
        '--config', type=str, help=f"Config file in yaml format", required=False
        )
    adar(
        '--edit','-e', action='store_true', help=f"Edit the config file with the default editor (if --config is not provided, will edit the auto-generated config file)", default=False
    )
    # adar(
    #     '--host', type=str, help=f"IP of host", default='0.0.0.0'
    #     )
    # adar(
    #     '--port', type=int, help=f"Port", default=8000
    #     )
    # Parse arguments
    args=par.parse_args(argv)
    config_file=args.config
    edit=args.edit
    # host=args.host
    # port=args.port

    def my_exit(msg):
        print(msg)
        sys.exit(1)

    if config_file is not None:
        if os.path.realpath(config_file)==os.path.realpath(DEFAULT_CONFIG_FILE):
            my_exit(f"Your config file has the automatic name. Please change the name so it is not overwritten: {DEFAULT_CONFIG_FILE}")
        assert os.path.isfile(config_file), f"Not a file {config_file}"
    else:
        config_file=make_auto_config_file(host='0.0.0.0', port=8000)
        if edit:
            from subprocess import call
            EDITOR = os.environ.get('EDITOR',None)
            if not EDITOR:
                EDITOR='vim'
                warn(f"No default editor found in environment variable $EDITOR. Defaulting to {EDITOR}.")
            call([EDITOR, config_file])
        my_exit(f"No config file provided. Created a template config file at {config_file}. Please edit it and run again.")

    main_internal(config_file)

if __name__=='__main__':
    main()



