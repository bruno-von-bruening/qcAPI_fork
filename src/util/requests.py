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
        response_text=response.text
        try:
            detail=json.loads(response_text)['detail']
        except Exception:
            detail=response_text
        raise Exception(f"Response failed with error code {status_code} (request_code=\'{request_code}\'):{detail}")

pdtc_address=str
@val_call
def make_url(srv_adress:pdtc_address, tag:str, opts:dict={} ):
    def make_opts(k,v):
        return f"{k}={v}"
    
    opts_str=[]
    for k,v in opts.items():
        if isinstance(v, list):
            opts_str+=[ make_opts(k,x) for x in v ]
        else:
            opts_str+=[ make_opts(k,v) ]
    opts_str= ( f"?{'&'.join(opts_str)}" if len(opts_str)>0 else '' )
    request_code=os.path.join(srv_adress,tag,opts_str)
    return request_code