#!/usr/bin/env python

from . import *
from server_external.launch_server import main as main_internal, make_auto_config_file, DEFAULT_CONFIG_FILE

from .wrapper import add_client_args, add_property_arg, process_argv, process_client_args, get_property_args

@val_call
@process_argv
def main(argv:List[str]|Namespace):
    description=None
    epilog=None
    prog=None
    par=argparse.ArgumentParser(prog=prog, description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter,)

    par=add_client_args(par, require_config=False)

    adar=par.add_argument
    adar(
        '--edit','-e', action='store_true', help=f"Edit the config file with the default editor (if --config is not provided, will edit the auto-generated config file)", default=False
    )
    adar(
        '--run','-r', action='store_true', help=f"If edit is provided then run the server after editing", default=False
    )
    
    # Parse arguments
    args=par.parse_args(argv)
    address, config_file=process_client_args(args, require_config=False, server_running=False)
    edit=args.edit
    do_run=args.run
    # host=args.host
    # port=args.port

    def my_exit(msg):
        print(msg)
        sys.exit(1)
    if not edit and do_run: my_exit(f"Run flag is only to be provided if edit flag is set to!")

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
        config_name='config.yaml'
        if os.path.isfile(config_name):
            my_exit(f"File with name {config_name} exists. Dumping info under {config_file}")
        else:
            shutil.move(config_file, config_name)
            config_file=config_name
        if not do_run:
            my_exit(f"No config file provided. Created a template config file at {config_file}. Please edit it and run again.")

    main_internal(config_file)

if __name__=='__main__':
    main()



