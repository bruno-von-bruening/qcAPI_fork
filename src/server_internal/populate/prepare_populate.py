# These function should return a list of entries that are then populated in the generic_populate function

from . import *
from .util import pop_tracker

@val_call
def prep_molpol_pop(
    tracker:pop_tracker,
    ids:List[str]|Literal['all']|None=None, # The ids of wave funciton
    json:dict={},
) -> Tuple[pop_tracker,List[dict]]:
    """
    """
    the_object=Molecular_Polarizability
    try: # Get available ids for objects
        if ids is None:
            ids='all'
        ancestor=Wave_Function
        the_ids=tracker.get_ids_for_table(ancestor, ids=ids)
    except Exception as ex: raise my_exception(f"Problem in getting available ids for {the_object.__name__} population:", ex)

    # Get existing childs:
    query=select(the_object.wfn_id)
    existing_ids=tracker.session.exec(query).all()


    records=[]
    try: # Make new objects
        for the_id in the_ids:
            if the_id in existing_ids:
                tracker.id_tracker.add_omitted(the_id)
            else:
                records+=[the_object(
                    wfn_id=the_id,
                    approach='finite_field',
                    code='psi4',
                )]
    except Exception as ex: raise my_exception(f"Problem in preparing new {the_object.__name__} objects:", ex)

    return tracker,[ x.model_dump() for x in records]

@val_call
def prep_wfn_pop(
    tracker:pop_tracker, ids:List[str]|str='all', json:dict={}
):
    """ Prepare wave function population """

    try: # Parse arguments
        assert 'level_of_theories' in json.keys(), f"Expected key \'level_of_theories\' in provided json objects."
        try:
            lots=[ Wave_Function_pass(**x) for x in json['level_of_theories'] ]
        except Exception as ex:
            raise Exception(f"Could not process argument of 'level_of_theories' as list of {Wave_Function_pass} in provided json objects.")
        assert len(lots)>0, f"Did not provide any level_of_theory entries!"
    except Exception as ex: my_exception(f"Problem in preparing wave base objects:", ex)

    # make the objects
    try:
        selected_ids=tracker.get_ids_for_table(Conformation, ids)

        new_wfn=[]
        for the_id in selected_ids:
            for lot in lots:
                method=lot.method
                basis=lot.basis
                new_wfn+=[Wave_Function(conformation_id=the_id, method=method, basis=basis, proctol=None).model_dump()]

    except Exception as ex: my_exception(f"Problem in populationg conformations",ex)

    return tracker,new_wfn

def prep_compound_pop(
    tracker:pop_tracker, ids:None, json:dict
)-> Tuple[pop_tracker,List[dict]]:

    try:
        def check_in_json(key):
            if not key in json.keys():
                raise Exception(f"Expected key \'{key}\' in provided json objects.")
            else: return json[key]

        compounds=check_in_json('records')
        assert all( isinstance(x, dict) for x in compounds), f"Expected list of dictionaries in provided json objects."

    except Exception as ex: my_exception(f"Problem in preparing compound population:", ex)

    # This is a root objects no ancestors to check
    records=[]
    try:
        for compound_raw in compounds:
            records+=[
                Compound(
                    inchikey=compound_raw['inchikey'],
                    elements=compound_raw['elements'],
                    charge=compound_raw.get('charge',0),
                    multiplicity=compound_raw.get('multiplicity',1),
                    formula=compound_raw.get('formula',None),
                    smiles=compound_raw.get('smiles',None),
                    mass=compound_raw.get('mass',None),
                    bonds=compound_raw.get('bonds',None),
                )
            ]
    except Exception as ex: my_exception(f"Problem in interpreting input as compounds",ex)

    return tracker, [ x.model_dump() for x in records ]

def prep_conformation_pop(
        tracker:pop_tracker, ids:List[str]|str='all', json:dict={}                  
)-> Tuple[pop_tracker,List[dict]]:
    
    # This are root entries
    try:
        assert 'records' in json.keys(), f"Expected key \'records\' in provided json objects. Found: {json.keys()}"
        conformations_raw=json['records']
        assert all( isinstance(x, dict) for x in conformations_raw), f"Expected list of dictionaries in provided json objects."
    except Exception as ex: my_exception(f"Problem in preparing population of {Conformation}:", ex)

    records=[]
    try:
        selected_ids=tracker.get_ids_for_table(Compound, 'all')
        for conf in conformations_raw:
            id_key_var='compound_id'
            id_key=conf.get('compound_id',None)
            if id_key is None:
                raise Exception(f"Expected key \'{id_key_var}\' in conformation entry. available keys: {conf.keys()}")
            elif id_key not in selected_ids:
                tracker.id_tracker.add_prerequisites_not_met(id_key) 
            else:
                records+=[
                    Conformation(**conf)
                ]

        if len(tracker.id_tracker.prerequisites_not_met) >0:
            raise Exception(f"For {len(tracker.id_tracker.prerequisites_not_met)} of the provided conformations, the parent compound_id was not found in database: {tracker.id_tracker.prerequisites_not_met}")
    except Exception as ex: my_exception(f"Problem in interpreting input as {Conformation} entries",ex)

    return tracker, [ x.model_dump() for x in records ]
