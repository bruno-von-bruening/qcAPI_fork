from . import *

from pydantic import BeforeValidator
from typing import Annotated

@validate_call
def check_server_responsiveness(address:str):
    try:
        requests.get(address)
    except Exception as ex:
        raise Exception(f"Could not reach server under address {address}")

@validate_call
def check_address(address:str):
    """ An addresss should have the from http://<hostname>:<port>/ (default port would be 80 but for the moment we will not use that)
    http is implicit and can be defaulted to
    """
    assert ':' in address, f"Expected \':\' character in address since it is required for port"
    if not address.startswith('http://'):
        address=f"http://{address}"

    check_server_responsiveness(address)

    return address 

pdtc_address=Annotated[ str, BeforeValidator(check_address)]