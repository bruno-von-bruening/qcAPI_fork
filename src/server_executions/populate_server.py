import pickle
from fastapi.encoders import jsonable_encoder
from util.util import auto_inchi
from qc_global_utilities.encoding_and_conversion.encoding import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol

from . import *

@validate_call
def indent(text:List[str]|str,indent_length:int=4, indent_character=' ', line_length=120):
    import textwrap
    the_indent=indent_length * indent_character
    break_length=line_length-len(the_indent)

    # We allways start with one block of text
    if isinstance(text, list):
        text='\n'.join(text)

    # break to lon lines 
    broken_text='\n'.join([ '\n'.join(textwrap.wrap(line,width=break_length)) for line in text.split('\n')])
    # indent text
    indented_text=textwrap.indent(broken_text, the_indent)

    return indented_text

def process_return(response):
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

def make_wfn(filenames,address, method, basis, do_test=False):
    """" """

    def get_conformations(filenames, do_test=do_test):
        # Load conformation from files
        conformations = []
        for filename in filenames:
            with open(filename, 'rb') as f:
                conformations += pickle.load(f)
        # If a test is requested we pick only the first 3 conformations (sorted by size)
        if do_test:
            #conformations=sorted( conformations, key=lambda x:len(x['species']))[:1]
            conformations=[ {'coordinates':[np.zeros(3) ,[0,0,-.74],np.ones(3)], 'species':[1, 1,6]}]
        # Jsonify the conformation
        conformations = jsonable_encoder(conformations,custom_encoder={np.ndarray: lambda x: x.tolist()})
        print(f"Number of supplied conformation found in {' '.join(filenames)}: {len(conformations)}")
        return conformations
    conformations=get_conformations(filenames, do_test=do_test)
    
    def make_conformation(conformations):

        # get inchikey
        for conformation in conformations:
            coordinates=np.array(conformation['coordinates'], dtype=np.float64)
            species=[ nuclear_charge_to_element_symbol(x) for x in conformation['species'] ]

            inchi, inchi_key=auto_inchi(coordinates, species)
            compound= dict(
                inchi=inchi, 
                inchikey=inchi_key,
                source='unkown',
                comments='conenctivity automapgenerated',
                bonds='auto',
                )
            conformation.update({'compound':compound})

        # Post and print what has been copied
        request_code=f"{address}/populate/conformation"
        response = requests.post(request_code, json={'conformations':conformations, 'ids':None})
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed request ({request_code}): status_code={status_code}, details=\'{response.text}\'")
        try:
            response_content=response.json()
            first_id = response_content["ids"]['succeeded'][0]

            request_str=f"{address}/get/conformation?ids={first_id}"
            load_request = requests.get(request_str)
            status_code=load_request.status_code
            if status_code!=HTTPStatus.OK:
                raise Exception(f"Failed request ({request_str}):\n status_code={status_code}, details=\'{load_request.text}\'")
            else:
                conformation=load_request.json()
                print(f"Following record has been set on server database (first as exmaple): {conformation}")
        except Exception as ex:
            raise Exception(f"Failed to process content: {ex} \n {response_content}")
        return response_content['ids']['succeeded'] # Only execution nothing to return
    def make_wave_functions(method: str, basis: str, conf_ids: List[int]):
        request_code=f"{address}/populate/wave_function"
        opts={'method':method, 'basis':basis}
        opts_str=[f"{k}={v}" for k,v in opts.items()]
        request_code+=f"?{'&'.join(opts_str)}"
        kwargs={'json': {'ids':conf_ids}}
        response=requests.post(request_code, **kwargs)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed request({request_code}):\n status_code={status_code}, detail=\'{response.text}\'")
        # , json=conf_ids)

    conf_ids=make_conformation(conformations)

    wfn_ids=make_wave_functions(method, basis, conf_ids)


from util.util import part_method_choice
def make_part(address, method:part_method_choice, basis:str|None=None,  do_test=False):
    """ """
    response = requests.post(f"{address}/populate/part?method={method}&basis={basis}", json={'ids':'all'})
    process_return(response)

    return None # Only execution nothing to return

def post_populate(request_code, json=None):
    response = requests.post( request_code, json=json)
    process_return(response)
def make_grid(address, do_test=False):
    """"""
    if do_test:
        pairs=[(1.e-4, 0.5)]
    else:
        pairs=[ 
                (1.e-2,0.2),
                #(3.e-7,0.5),
                #(1.e-7,0.5),
                #(3.e-2,0.2)
                #(1.e-2,0.2)
                #(1.e-6, 0.5),
                #(3.e-6, 0.5),
                #(1.e-5, 0.4),
                #(2.e-4, 0.4),
                #(1.e-3, 0.1),
        ]
        #pairs=[ 
        #    (1.e-3, 0.1 ), 
        #    (2.e-3, 0.1 ), 
        #    (5.e-3, 0.1 ), 
        #    (1.e-4, 0.2 ), 
        #    (2.e-4, 0.2 ), 
        #    (5.e-4, 0.2 ), 
        #    (1.e-5, 0.4 ), 
        #    (2.e-5, 0.4 ), 
        #    (5.e-4, 0.4 ),
        #]

    request_code=f"{address}/populate/isosurf"
    the_json={'grid_pairs':pairs}
    post_populate(request_code, json=the_json)

def make_rhoesp(address, do_test=False):
    """ """
    request_code=f"{address}/populate/density_esp"
    the_json={}
    post_populate(request_code, json=the_json)
def make_dmpesp(address, do_test=False):
    """ """
    request_code=f"{address}/populate/multipolar_esp"
    the_json={}
    post_populate(request_code, json=the_json)
       
def make_espcmp(address, do_test=False):
    """ """
    request_code=f"{address}/populate/compare_esp"
    the_json={}
    post_populate(request_code, json=the_json)

def make_group(address, content_file:str, do_test=False):
    """ """
    request_code=f"{address}/populate/group"

    assert os.path.isfile(content_file)
    extension=os.path.basename(content_file).split('.')
    assert len(extension)>1
    extension=extension[-1].lower()

    with open(content_file, 'r') as rd:
        if extension=='json':
            data=json.load(rd)
        elif extension=='yaml':
            data=yaml.safe_load(rd)
        else:
            raise Exception(f"Unkown extension of file \'{os.path.realpath(content_file)}\': {extension}")
    
    post_populate(request_code, json={'records':data})

def main(filenames,address, property, method, basis, do_test=False, content_file=None):
    """ Switch dependant on which property to compute"""
    UNIQUE_NAME=get_unique_tag(property)
    if UNIQUE_NAME==NAME_WFN:
        assert all([ os.path.isfile(x) for x in filenames ])
        make_wfn(filenames, address, method, basis, do_test=do_test)
    elif UNIQUE_NAME==NAME_PART:
        make_part(address, method, basis, do_test=do_test)
    elif UNIQUE_NAME==NAME_IDSURF:
        make_grid(address,do_test=do_test)
    elif NAME_ESPRHO==UNIQUE_NAME:
        make_rhoesp(address,do_test=do_test)
    elif NAME_ESPDMP==UNIQUE_NAME:
        make_dmpesp(address, do_test=do_test)
    elif NAME_ESPCMP==UNIQUE_NAME:
        make_espcmp(address, do_test=do_test)
    elif NAME_GROUP==UNIQUE_NAME:
        make_group(address, content_file=content_file, do_test=do_test)
    else:
        raise Exception(f"No case implemented for handling property {property}")
