#!/usr/bin/env python

from util.util import available_operations, OP_DELETE, OP_CLEAN_DOUBLE, OP_CLEAN_PENDING, available_properties, check_address

import requests
from http import HTTPStatus

def main(mode, address, prop, force=False):
    # map mode to function
    if OP_DELETE == mode:
        request_code=f"{address}/delete/{prop}?force={force}"
        response=requests.post(request_code)
        return_code=response.status_code
        if return_code != HTTPStatus.OK:
            raise Exception(f"Http request ({request_code}) failed with code {return_code}: {response.text}")
    if OP_CLEAN_DOUBLE == mode:
        request_code=f"{address}/clean_double/{prop}?force={force}"
        response=requests.post(request_code)
        return_code=response.status_code
        if return_code != HTTPStatus.OK:
            raise Exception(f"Http request ({request_code}) failed with code {return_code}: {response.text}")
    elif OP_CLEAN_PENDING == mode:
        request_code=f"{address}/clean_pending/{prop}"
        response=requests.post(request_code)
        return_code=response.status_code
        if return_code != HTTPStatus.OK:
            raise Exception(f"Http request ({request_code}) failed with code {return_code}: {response.text}")




        

if __name__=="__main__":
    description=None
    epilog=None
    prog=None
    import argparse; par=argparse.ArgumentParser(description=description, epilog=epilog, prog=prog)
    add = par.add_argument
    add(
        'MODE', type=str, choices=available_operations,
        help=f"Mode to be executed",
    )
    add(
        '--address', type=str, default='0.0.0.0:8000',
        help=f"Server address (ip:port)"
    )
    add(
        '--property', '-p', required=True, #choices=available_properties, required=True,
        help=f"Name of property to be deleted", 
    )
    add(
        '--force', action='store_true', help=f"In case there are critical changes that caused wanring use this flag to force their execution"
    )
    #
    args=par.parse_args()
    mode=args.MODE 
    prop=args.property
    address=f"http://{args.address}"
    force=args.force

    # Check validty of 
    from util.util import get_unique_tag
    prop=get_unique_tag(prop, print_options=True)


    check_address(address)
    main(mode, address, prop, force=force)