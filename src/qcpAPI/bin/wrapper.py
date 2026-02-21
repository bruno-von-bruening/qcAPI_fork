from . import *

from util.config import qcAPI_server_config
from util.http_util import pdtc_address

def process_argv(func):
    """Decorator: pre‑process argv so that --property is mapped to a unique tag."""
    def wrapper(argv=None, *args, **kwargs):
        
        # Default take arguments from sys.argv
        argv = argv if argv is not None else sys.argv[1:]

        # Work on a copy so caller's list is untouched
        new_argv = list(argv)
            


        return func(new_argv, *args, **kwargs)
    return wrapper

from argparse import ArgumentParser, Namespace

def add_client_args(par:ArgumentParser, require_config=False):
    if not require_config:
        mex=par.add_mutually_exclusive_group(required=True)
        mex.add_argument( f"--config", "-c", type=str, help="Client yaml file to use" )
        mex.add_argument( f"--address", "-a", type=str, help="Address of the server in URL:PORT format (could also be read from client yaml file!)" )
    else:
        par.add_argument( f"--config", "-c", type=str, help="Client yaml file to use", required=True )
    return par
def add_property_arg(par:ArgumentParser):
    par.add_argument( f"--property", "-p", type=str, help="Which property is of interest", required=True )
    return par
def get_property_args(pre_args:Namespace) -> str:
    return pre_args.property

@val_call
def process_client_args(
    pre_args:Namespace, require_config=False, server_running=True
) -> str|tuple[str, pdtc_file]:
    from util.http_util import check_address
    if require_config:
        conf=qcAPI_server_config(pre_args.config)
        address=conf.address
        check_address(address, negate=not server_running)
        return address, pre_args.config
    else:
        if pre_args.config is not None:
            #from qcp_global_utils.environment.file_handling import load_json_or_yaml
            conf=qcAPI_server_config(pre_args.config)
            address=conf.address
        elif pre_args.address is not None:
            address=pre_args.address
        else: raise Exception(f"Either --config or --address must be provided!") # But should not be possible due to mutually exclusive group

        check_address(address, negate=not server_running)
        return address