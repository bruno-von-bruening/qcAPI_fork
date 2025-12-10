
# sent restart signal to server (restart should be handled by major run server process as its received)

from . import *

from .wrapper import process_argv, process_client_args, add_client_args

def main_restart(server_address:str):
    import requests
    from http import HTTPStatus

    url=f"{server_address}/restart_server"
    try:
        resp=requests.post(url)
    except Exception as ex:
        raise Exception(f"Error connecting to server at {server_address} to send restart command: {ex}")

    if resp.status_code != HTTPStatus.OK:
        raise Exception(f"Error response from server at {server_address} when sending restart command: {resp.status_code} {resp.text}")
    else:
        print(f"Restart command sent to server at {server_address} successfully.")

@validate_call
@process_argv
def main(argv:Namespace|List[str]):
    description="Send restart command to server"
    epilog=None
    prog=None
    par=ArgumentParser(description=description, epilog=epilog, prog=prog, formatter_class=argparse.RawDescriptionHelpFormatter,)
    add_client_args(par)

    args=par.parse_args(argv)
    server_address=process_client_args(args)
    
    main_restart(server_address)
