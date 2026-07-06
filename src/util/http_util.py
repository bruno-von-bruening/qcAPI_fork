from . import *
import requests

def check_address(address):
    import requests
    try:
        response=requests.get(address)
    except Exception as ex:
        raise Exception(f"Cannot communicate with address ({address}):\n {ex}")


@val_call
def check_server_responsiveness(address:str, timemout:float|int=6, negate=False):
    """ If negate I actually expect the command to succeed """
    try:
        response=requests.get(address, timeout=timemout)
    except requests.exceptions.Timeout as ex:
        raise ex
    except Exception as ex:
        if not negate:
            raise Exception(f"Could not reach server under address {address}: {ex}")
        return
    
    if response.ok and negate:
        raise Exception(f"Server under address {address} should not be running already!")
    
@val_call
def check_address(address:str, negate=False, 
                  timeout=6, repeats=1, wait=60):
    """ An addresss should have the from http://<hostname>:<port>/ (default port would be 80 but for the moment we will not use that)
    http is implicit and can be defaulted to

    """
    assert ':' in address, f"Expected \':\' character in address since it is required for port"
    if not address.startswith('http://'):
        address=f"http://{address}"


    # check_server_responsiveness(address,negate=negate)
    for i in range(repeats):
        try:
            check_server_responsiveness(address, timeout, negate=negate)
            timeout_error=False
        except requests.exceptions.Timeout:
            timeout_error=True
            if i+1>=repeats:
               break 
            else:
                time.sleep(wait)
        except Exception as ex:
            raise ex
    if timeout_error:
        error=\
        f"Server under {address} can be reached but does not respond (possibly due to large workload)" + \
        f"(after {repeats} repeats with wait={wait} [s] and timeout_tollerance={timeout})\n" + \
        f"Hence, terminating!"
        raise Exception(error)

    return address 

pdtc_address=Annotated[ str, BeforeValidator(check_address)]
from functools import partial
pdtc_address_tollerant=Annotated[ str, BeforeValidator( partial(check_address, repeats=10))]