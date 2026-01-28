from .. import *
from util.util import part_method_choice
from util.sql_util import *
from orm_import.database_declaration import Conformation

from util.type_helpers.data_types import Wave_Function_pass
def wave_functions_url(
        records: List[Wave_Function_pass],
        conf_ids: List[str]|str='all', 
        do_test:bool=False
):
    request_code=f"populate/wave_function"
    opts={'json':dict(
        records=records,
    )}
    the_json={'ids':conf_ids, 'records':records}
    return opts, the_json

def conformations_url(conformations):
    """ Given a list of conformations generate """
    # get inchikey

    # Post and print what has been copied
    request_code=f"{address}/populate/conformation"
    the_json={'conformations':conformation,'ids':None}
    return request_code, the_json

@val_call
def load_pubchem_data(inchi_keys:List[str], inchi_mapper:dict|None=None):
    from helper.pubchempy_handler import pubchem_handler, load_compounds_from_pubchem
    print(f"Loading {len(inchi_keys)} compounds from pubchem"); start=time.time()
    comps=load_compounds_from_pubchem(inchikeys=inchi_keys, inchi_mapper=inchi_mapper)
    print(f"Loading from pubchem took {time.time()-start:.2f} second")
    print(f"Repacking Compounds"); tmp=time.time()
    comps=[ pubchem_handler(input=c).to_database_entry() for c in comps]
    print(f"Repacking took {time.time()-tmp:.2f} seconds")
    return comps


@val_call
def compounds_url( 
    records: List[dict],
    do_test=False
):
    the_json={}
    the_json={'records':records, 'inchikeys':[], 'compound_ids':[]}
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

@val_call
def molpol_url(ids:List[str]|str='all', records:List[Molecular_Polarizability]=[], codes:List[Code]=[], do_test=False):
    """ """
    opts=dict(
        ids=ids,
    )
    the_json=dict(
        records=[r.model_dump() for r in records],
        codes=[c.model_dump() for c in codes]
    )
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
    
    # Get the code entry
    the_json={'records':data}
    opts={}
    return opts, the_json


@val_call
def conformations_url(
    records: List[dict],
    do_test: bool=False,
):
    confs=[]
    for rec in records:
        try:
            confs+=[ Conformation(**rec).model_dump() ]
        except Exception as ex: raise Exception(f"Could not generate record for {rec}: {ex}")

    the_json={'records':confs} 
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
    #
    NAME_MOLPOL: molpol_url,
}

@val_call
def get_url_func(tag:str|SQLModelMetaclass):

    if isinstance(tag, SQLModelMetaclass):
        tag=get_unique_tag(tag)

    assert tag in url_funcs_map.keys(), f"Key \'{tag}\' not in available keys: {list(url_funcs_map.keys())}"
    return url_funcs_map[tag]

