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
    try:
        response = requests.post( request_code, json=json)
    except requests.exceptions.ConnectionError as hex: # ConnectionError shadows the built-in class!
        raise Exception(f"Could not connect to server, is it running? {hex}")
    process_return(response)
def my_quit(msg):
    print(msg)
    sys.exit(1)


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

@val_call
def main(filenames:List[file_pdtc],address, property:str, method:str|None=None, basis:str|None=None, do_test=False):
    """ Switch dependant on which property to compute"""

    UNIQUE_NAME=get_unique_tag(property)

    if len(filenames)>0:
        content, content_file= process_arguments(filenames)
    else:
        content=None
        content_file=None

    func=get_url_func(UNIQUE_NAME)
    
    if NAME_COMP==UNIQUE_NAME:
        inchikey_tag='inchi_keys'
        from .populate_extension import load_pubchem_data
        if not content is None:
            assert inchikey_tag in content.keys(), f"Expected key \'{inchikey_tag}\' in \'{content_file}\'"
            inchi_keys=content[inchikey_tag]
        else: 
            my_quit(f"Provide a file in which you dropped a dictionary with key \'{inchikey_tag}\' that holds a list of inchikeys")

        compounds=load_pubchem_data(inchi_keys)
        kwargs=dict(records=compounds)
    elif NAME_CONF==UNIQUE_NAME:
        records=[]
        if content is None:
            my_quit(f"No content provided, nothing todo.")
        elif isinstance(content, list):
            records+=content
        elif isinstance(content, dict):
            assert 'records' in content.keys(), f"Expected \'records\' in \'{content_file}\'"
            assert all( isinstance(x, dict) for x in content['records'] ), f"Expected list of dictionaries in \'records\' in \'{content_file}\'"
            records+=content['records']


        rec_ref=[]
        for rec in records:
            try:
                rec_ref+=[ Conformation(**rec) ]
            except Exception as ex: 
                required_keys=['inchikey','geometry']
                cnt=sum([ k in rec.keys() for k in required_keys ])
                if cnt==len(required_keys):
                    from qcp_objects.objects.properties import geometry
                    geom=geometry(rec['geometry'])
                    geom.units.LENGTH='BOHR'
                    coords=geom.coordinates.reshape(-1)
                    elements=geom.atom_types
                    

                    inchi, inchi_key=auto_inchi(geom.coordinates, geom.atom_types)
                    if 'inchikey' in rec.keys():
                        if rec['inchikey'].lower() == 'auto':
                            pass
                        else:
                            assert inchi_key==rec['inchikey'], f"Provided inchikey \'{rec['inchikey']}\' does not match generated inchikey \'{inchi_key}\' from geometry!"
                    

                    rec_ref+=[ Conformation(
                        compound_id=inchi_key,
                        coordinates=coords, elements=elements,
                    )]

                else: raise Exception(f"Could not generate record for {rec}: {ex}")
        inchis=[ r.compound_id for r in rec_ref ]
        from receiver.get_request import get_row
        entries=get_row(address, 'compound', ids=list(set(inchis)) )
        existing_inchis=[ r[get_primary_key_name(Compound)] for r in json.loads(entries['json'])['record'] ]
        missing_inchis=[ x for x in inchis if x not in existing_inchis ]

        from .populate_extension import load_pubchem_data
        if len(missing_inchis)>0:
            compounds=load_pubchem_data(missing_inchis)


            opts, json_content= get_url_func(Compound)(records=compounds)
            request_body=f"populate/{get_unique_tag(Compound).lower()}"
            request_code=make_url(address, request_body, opts)
            print(f"Posting request code: {request_code}")

            post_populate(request_code, json=json_content)

        #json_content=dict(records=[x.model_dump() for x in rec_ref])        
        kwargs=dict(records=[x.model_dump() for x in rec_ref])
        
    elif NAME_WFN==UNIQUE_NAME:
        #assert all([ os.path.isfile(x) for x in filenames ])
        from util.type_helpers.data_types import Wave_Function_pass
        filenames=[]

        if content is not None:
            try:
                assert isinstance(content, list), f"Expected list of level_of_theory entries in file \'{content_file}\'"
                assert all( isinstance(x, dict) for x in content), f"Expected list of dictionaries in file \'{content_file}\'"
                lots= [ Wave_Function_pass(**x) for x in content ]
            except Exception as ex: raise Exception(f"Could not process content of file \'{content_file}\' as {level_of_theory_entries}:\n{ex}")
        else:
            lots=[]
        
        cnt=sum([ x is not  None for x in [method,basis] ])
        if cnt==0:
            pass
        elif cnt==1:
            my_quit(f"Provided only method or basis set but both needed to make wave function level of theory.")
        else: # cnt==2
            lots+=[ Wave_Function_pass(method=method, basis=basis)]
        kwargs=dict(level_of_theories=[ x.model_dump() for x in lots],conf_ids='all')
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
    elif NAME_MOLPOL==UNIQUE_NAME:
        kwargs={}
    else:
        raise Exception(f"No case implemented for handling property {property}")

    
    opts, json_content= func(**kwargs)
    request_body=f"populate/{UNIQUE_NAME.lower()}"
    request_code=make_url(address, request_body, opts)
    print(f"Posting request code: {request_code}")

    post_populate(request_code, json=json_content)