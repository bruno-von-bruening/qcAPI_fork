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