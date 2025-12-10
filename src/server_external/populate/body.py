import pickle
from fastapi.encoders import jsonable_encoder
from util.util import auto_inchi
from util.auxiliary import indent
from qcp_global_utils.encoding_and_conversion.encoding import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol

from .. import *
from .populate_extension import get_url_func

def process_return(response):
    """ Print message dependant on the return from the server (success code) """
    status_code=response.status_code
    if status_code!=HTTPStatus.OK:
        detail=str(response.json()['detail'])
        raise Exception(f"Request \'{response.url}\' failed with code {status_code}:\n{indent(detail,indent_length=1, indent_character='|   ')}")
    else:
        json_obj=response.json()
        if json_obj is not None:
            if 'message' in json_obj:
                message=indent(json_obj['message'])
                print(f"Successful population with message:\n{message}")
            else:
                print(f"Successful population without message.")
        else:
            print(f"Successful population without message.")


def post_populate(request_code, json=None):
    try:
        response = requests.post( request_code, json=json)
    except requests.exceptions.ConnectionError as hex: # ConnectionError shadows the built-in class!
        raise Exception(f"Could not connect to server, is it running? {hex}")
    process_return(response)


@val_call
def process_arguments(filenames:List[file_pdtc]):
    if len(filenames)>0:
        if len(filenames)>1: raise Exception(f"Implement merging of file information")
        else: content_file=filenames[0]
        try:
            content=load_json_or_yaml(content_file)
        except Exception as ex: raise Exception(f"Could not read \'{content_file}\': {ex}")
    else:
        content=None
    return content, content_file
