#!/usr/bin/env python

from util.util import available_operations, OP_DELETE, OP_CLEAN_DOUBLE, OP_CLEAN_PENDING, OP_RESET, available_properties, check_address

import requests
from http import HTTPStatus
from pydantic import validate_call
from typing import List
import re


separator='__'
@validate_call
def process_filters(input: List[str]):
    f""" 
    Expects list of argument separated by \'{separator}\' each forming a dictionary entry
    """
    dic=[]
    for it in input:
        assert re.search(separator, it), f"Expect \'{separator}\' between dictionary key and item"
        ar=it.split(separator)
        assert len(ar)==2, f"Expected two argument (key and value separated by {separator}), got: {it}"
        dic.append(ar)
    return dict(dic)

def main(mode, address, prop, force=False, filters=None):
    """ """
    if filters is None:
        filters={}
    else:
        filters=process_filters(filters)

    # map mode to function
    def make_request(code, args={}, filters={}, test=False):
        """ """

        # Base of request code
        request_code=f"{address}/{code.lstrip('/')}"
        # optional arguments
        @validate_call
        def pack_dic(dic:dict):
            """ Dict to list of key to value strings"""
            new_dic=[]
            for k,v in filters.items():
                new_dic.append(f"{k}--{v}")
            return new_dic
        
        if len(filters)>0:
            assert not 'filters' in args, f"Keyword filters is already in arguments"
            args.update({'filters':filters})
        if len(args)>0:
            new_args=[]
            for k,arg in args.items():
                if isinstance(arg, dict):
                    new_args+=[ f"{k}={x}" for x in pack_dic(arg)]
                elif isinstance(arg, (float, int,str)):
                    new_args+=[ f"{k}={arg}"]
            request_code+='?'+'&'.join(new_args)

        if test:
            print(request_code)
            quit(f"Planned exit after test")
        else:
            response=requests.post(request_code)
            return_code=response.status_code
            if return_code != HTTPStatus.OK:
                raise Exception(f"Http request ({request_code}) failed with code {return_code}: {response.json()['detail']}")
            else:
                js=response.json()
                
                if js is None:
                    message=f"Return without message"
                elif 'message' in js:
                    message=f"Return with message: {js['message']}"
                else:
                    message=f"Return without message"
                print(message)
    if OP_DELETE == mode:
        make_request(f"/delete/{prop}", args={'force':force}, filters=filters, test=False)
    elif OP_CLEAN_DOUBLE == mode:
        make_request(f"/clean_double/{prop}", args={'force':force}, filters=filters, test=False)
    elif OP_CLEAN_PENDING == mode:
        request_code=f"{address}/clean_pending/{prop}"
        response=requests.post(request_code)
        return_code=response.status_code
        if return_code != HTTPStatus.OK:
            raise Exception(f"Http request ({request_code}) failed with code {return_code}: {response.text}")
    elif OP_RESET == mode:
        make_request(f"/reset/{prop}", args={'force':force}, filters=filters)
    else:
        raise Exception(f"Did not recognize {mode}")





        

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
    add(
        '--filters', nargs='+', help=f"Filter Table for the given properties"
    )
    #
    args=par.parse_args()
    mode=args.MODE 
    prop=args.property
    the_filters=args.filters
    address=f"http://{args.address}"
    force=args.force

    # Check validty of 
    from util.util import get_unique_tag
    prop=get_unique_tag(prop, print_options=True)


    check_address(address)
    main(mode, address, prop, force=force, filters=the_filters)