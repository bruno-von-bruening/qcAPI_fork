from . import *

import requests
from http import HTTPStatus

@validate_call
def get_request(request_code:str):
    
    response=requests.get(request_code)
    
    status_code=response.status_code
    if status_code==HTTPStatus.OK:
        return response
    else:
        raise Exception(f"Response failed with error code {status_code} (request_code=\'{request_code}\'):{response.text}")

pdtc_address=str
@val_call
def make_url(srv_adress:pdtc_address, tag:str, opts:dict={} ):
    
    opts_str=[f"{k}={v}" for k,v in opts.items()]
    opts_str= ( f"?{'&'.join(opts_str)}" if len(opts_str)>0 else '' )
    request_code=os.path.join(srv_adress,tag,opts_str)
    return request_code