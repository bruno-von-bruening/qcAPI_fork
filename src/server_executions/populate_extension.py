from . import *
from util.util import part_method_choice

def wave_functions_url(method: str, basis: str, conf_ids: List[str]|str='all', do_test:bool=False):
    request_code=f"populate/wave_function"
    opts={'method':method, 'basis':basis}
    the_json={'ids':conf_ids}
    return opts, the_json

def conformations_url(conformations):
    """ Given a list of conformations generate """
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
    the_json={'conformations':conformation,'ids':None}
    return request_code, the_json

@val_call
def compounds_url( 
    inchi_keys:List[str], 
    do_test=False
):
    the_json={}
    from helper.pubchempy_handler import pubchem_handler, load_compounds_from_pubchem
    print(f"Loading {len(inchi_keys)} compounds from pubchem"); start=time.time()
    comps=load_compounds_from_pubchem(inchikeys=inchi_keys)
    print(f"Loading from pubchem took {time.time()-start:.2f} second")
    print(f"Repacking Compounds"); tmp=time.time()
    comps=[ pubchem_handler(input=c).to_database_entry() for c in comps]
    print(f"Repacking took {time.time()-tmp:.2f} seconds")
    the_json={'records':comps, 'inchikeys':[], 'compound_ids':[]}
    opts={}
    return opts, the_json

def partitionings_url(method:part_method_choice, basis:str|None=None,  do_test=False):
    """ """
    opts=dict(
        method=method,
        basis=basis,
    )
    the_json={'ids':'all'}
    return opts, the_json
def isodsurf_url(do_test=False):
    """"""
    if do_test:
        pairs=[(1.e-4, 0.5)]
    else:
        pairs=[ 
                (1.e-2,0.2),
                #(3.e-7,0.5) camcasp_script,camcasp_ {}camcaspcamcasp_script,
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

    the_json={'grid_pairs':pairs}
    opts={}
    return opts, the_json

def rhoesp_url(do_test=False):
    """ """
    opts={}
    the_json={}
    return opts, the_json
def dmpesp_url(do_test=False):
    """ """
    opts={}
    the_json={}
    return opts, the_json
       
def espcmp_url(do_test=False):
    """ """
    opts={}
    the_json={}
    return opts, the_json

def groups_url(content_file:str, do_test=False):
    """ """

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
    
    the_json={'records':data}
    opts={}
    return opts, the_json


@val_call
def conformations_url(
    records: List[dict],
    do_test: bool=False,
):

    from qcp_database.linked_tables import Conformation

    confs=[]
    for rec in records:
        try:
            confs+=[ Conformation(**rec).model_dump() ]
        except Exception as ex: raise Exception(f"Could not generate record for {rec}: {ex}")

    the_json={'conformations':confs} 
    opts={}
    return opts, the_json

@val_call
def dispol_url(
    do_test=False,
):
    opts={}
    the_json={'ids':'all'}
    return opts, the_json

url_funcs_map={
    NAME_COMP: compounds_url,
    NAME_CONF: conformations_url,
    NAME_GROUP: groups_url,
    #
    NAME_WFN: wave_functions_url,
    NAME_PART: partitionings_url,
    NAME_DISPOL: dispol_url,
    #
    NAME_IDSURF: isodsurf_url, 
    NAME_ESPRHO: rhoesp_url,
    NAME_ESPDMP: dmpesp_url,
    NAME_ESPCMP: espcmp_url,
}
def get_url_func(tag):

    assert tag in url_funcs_map.keys(), f"Key \'{tag}\' not in available keys: {list(url_funcs_map.keys())}"
    return url_funcs_map[tag]

