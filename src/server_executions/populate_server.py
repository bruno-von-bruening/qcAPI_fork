import pickle
from fastapi.encoders import jsonable_encoder
from util.util import auto_inchi
from util.auxiliary import indent
from qcp_global_utils.encoding_and_conversion.encoding import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from qcp_global_utils.pydantic.pydantic import file as file_pdtc

from . import *
from .populate_extension import get_url_func
from util.requests import make_url

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
    response = requests.post( request_code, json=json)
    process_return(response)

#def make_wfn(filenames,address, method, basis, do_test=False):
#    """" """
#    def get_conformations(filenames, do_test=do_test):
#        # Load conformation from files
#        conformations = []
#        for filename in filenames:
#            with open(filename, 'rb') as f:
#                conformations += pickle.load(f)
#        # If a test is requested we pick only the first 3 conformations (sorted by size)
#        if do_test:
#            #conformations=sorted( conformations, key=lambda x:len(x['species']))[:1]
#            conformations=[ {'coordinates':[np.zeros(3) ,[0,0,-.74],np.ones(3)], 'species':[1, 1,6]}]
#        # Jsonify the conformation
#        conformations = jsonable_encoder(conformations,custom_encoder={np.ndarray: lambda x: x.tolist()})
#        print(f"Number of supplied conformation found in {' '.join(filenames)}: {len(conformations)}")
#        return conformations
#    conformations=get_conformations(filenames, do_test=do_test)
#    conf_ids=make_conformation(conformations)
#    wfn_ids=make_wave_functions(method, basis, conf_ids)
    #try:
    #    response_content=response.json()
    #    first_id = response_content["ids"]['succeeded'][0]

    #    request_str=f"{address}/get/conformation?ids={first_id}"
    #    load_request = requests.get(request_str)
    #    status_code=load_request.status_code
    #    if status_code!=HTTPStatus.OK:
    #        raise Exception(f"Failed request ({request_str}):\n status_code={status_code}, details=\'{load_request.text}\'")
    #    else:
    #        conformation=load_request.json()
    #        print(f"Following record has been set on server database (first as exmaple): {conformation}")
    #except Exception as ex:
    #    raise Exception(f"Failed to process content: {ex} \n {response_content}")
    #return response_content['ids']['succeeded'] # Only execution nothing to return
@val_call
def main(filenames:List[file_pdtc],address, property, method, basis, do_test=False):
    """ Switch dependant on which property to compute"""
    UNIQUE_NAME=get_unique_tag(property)

    if len(filenames)>0:
        if len(filenames)>1: raise Exception(f"Implement merging of file information")
        else: content_file=filenames[0]
        try:
            with open(content_file, 'r') as rd:
                if content_file.endswith('.yaml'):
                    content=yaml.safe_load(rd)
                elif content_file.endswith('.json'):
                    content=json.load(rd)
                else:
                    raise Exception(f"Cannot interpete file due to its extension (or lack off): {content_file}")
        except Exception as ex: raise Exception(f"Could not read \'{content_file}\': {ex}")
    else:
        content=None


    func=get_url_func(UNIQUE_NAME)
    if NAME_COMP==UNIQUE_NAME:
        inchikey_tag='inchi_keys'
        if not content is None:
            assert inchikey_tag in content.keys(), f"Expected key \'{inchikey_tag}\' in \'{content_file}\'"
            inchi_keys=content[inchikey_tag]
        else: 
            raise Exception(f"Provide a file in which you dropped a dictionary with key \'{inchikey_tag}\' that holds a list of inchikeys")
        kwargs=dict(inchi_keys=inchi_keys)
    
    elif NAME_CONF==UNIQUE_NAME:
        if not content is None:
            assert 'records' in content.keys(), f"Expected \'records\' in \'{content_file}\'"
        kwargs=dict(records=content['records'])

    elif NAME_WFN==UNIQUE_NAME:
        #assert all([ os.path.isfile(x) for x in filenames ])
        filenames=[]
        kwargs=dict(method=method, basis=basis, conf_ids='all')
    elif UNIQUE_NAME==NAME_PART:
        kwargs=dict(method=method, basis=basis)
    elif UNIQUE_NAME==NAME_IDSURF:
        kwargs={}
    elif NAME_ESPRHO==UNIQUE_NAME:
        kwargs={}
    elif NAME_ESPDMP==UNIQUE_NAME:
        kwargs={}
    elif NAME_ESPCMP==UNIQUE_NAME:
        kwargs={}
    elif NAME_GROUP==UNIQUE_NAME:
        kwargs=dict(content_file=content_file)
    elif NAME_DISPOL==UNIQUE_NAME:
        kwargs={}
    else:
        raise Exception(f"No case implemented for handling property {property}")

    
    opts, json_content= func(**kwargs)
    request_body=f"populate/{UNIQUE_NAME.lower()}"
    request_code=make_url(address, request_body, opts)
    print(f"Posting request code: {request_code}")

    post_populate(request_code, json=json_content)