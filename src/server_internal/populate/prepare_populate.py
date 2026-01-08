# These function should return a list of entries that are then populated in the generic_populate function

from . import *
from .util import pop_tracker

from sqlalchemy import UniqueConstraint
from sqlmodel import SQLModel, select

def get_unique_constraint_cols(model: type[SQLModel], name: str | None = None) -> list[str]:
    """
    Return the list of column names for a (named) UniqueConstraint on a SQLModel.
    If `name` is None, returns the first table‑level UniqueConstraint.
    """
    tbl = model.__table__

    constraints=[ c for c in tbl.constraints if isinstance(c, UniqueConstraint) ]  

    if len(constraints)==0:
        return []
    else:
        if name is not None:
            found=[ c for c in constraints if c.name==name ]
            assert len(found)==1,  f"Expected exactly one UniqueConstraint named {name} for {model.__name__}, found {len(found)}"
        else:
            found=constraints
            assert len(found)==1,  f"Expected exactly one UniqueConstraint for {model.__name__}, found {len(found)}"
        return [ c.name for c in found[0].columns ]

@val_call
def prep_molpol_pop(
    tracker:pop_tracker,
    ids:List[str]|Literal['all']|None=None, # The ids of wave funciton
    specs:dict={},
    json:dict={},
) -> Tuple[pop_tracker,List[dict]]:
    """
    """

    assert 'records' in json.keys(), f"Expected key \'records\' in provided json objects. Found: {json.keys()}"
    records_raw=json['records']    
    
    the_object=Molecular_Polarizability

    # Check that objects are valid
    for r in records_raw:
        try:
            the_object(**r, blank=True)
        except Exception as ex: raise my_exception(f"Problem in preparing population of {the_object.__name__}:", ex)

    try: # Get available ids for objects
        if ids is None:
            ids='all'
        ancestor=Wave_Function
        the_ids=tracker.get_ids_for_table(ancestor, ids=ids)
    except Exception as ex: raise my_exception(f"Problem in getting available ids for {the_object.__name__} population:", ex)

    # Get wfn ids that already exist
    constraints=get_unique_constraint_cols(the_object)
    query=select( *(getattr( the_object, c) for c in constraints) )
    existing_entries=tracker.session.exec(query).all()

    # Make the specs
    # approach=specs.get('approach', the_object.allowed_approaches.finite_field)
    #  if approach==Molecular_Polarizability.allowed_approaches.finite_field:
    #      specs_setup=Molecular_Polarizability.specs_model_ff
    #      specs_setup=specs_setup( finfie_stepsize_dip=1.e-3, finfie_stepsize_qad=1.e-4, eval_through=specs_setup.allowed_eval_from.energy )
    #  elif approach==Molecular_Polarizability.allowed_approaches.linear_response:
    #      specs_setup=Molecular_Polarizability.specs_model_lr
    #      specs_setup=specs_setup()
    #  else:
    #      raise Exception(f"Unknown approach provided for {the_object.__name__} population: {approach}")
    # kwargs_def=dict(
        # approach=approach,
        # code='psi4',
        # specs=specs_setup.model_dump(),
    # )
    # for k,v in specs.items():
    #     for r in records_raw:
    #         r[k]=v
        # if k in kwargs_def.keys():
        #     if isinstance(kwargs_def[k], dict):
        #         kwargs_def[k].update(v)
        #     else:
        #         kwargs_def[k]=v
        # else:
        #     kwargs_def[k]=v
    
    candidates=[]
    try: # Add all new ids
        for the_id in [ 
            x for x in the_ids 
        ]:
            for r in records_raw:
                kwargs_def=r.copy()
                kwargs_def['wfn_id']=the_id
                candidates+=[the_object(
                    **kwargs_def, blank=True
                )]
    except Exception as ex: raise my_exception(f"Problem in preparing new {the_object.__name__} objects:", ex)

    records=[]
    tracker.messanger.start_timing()
    try: # Filter out existing entries
        for cand in candidates:
            combo=tuple( getattr(cand, c) for c in constraints )
            if not combo in existing_entries:
                records+=[ cand ]
            else:
                tracker.id_tracker.add_omitted( cand.wfn_id )
    except Exception as ex: raise my_exception(f"Problem in filtering existing {the_object.__name__} objects:", ex)
    tracker.messanger.stop_timing(f"Get existing combos")

    return tracker,[ x.model_dump() for x in records]

@val_call
def prep_wfn_pop(
    tracker:pop_tracker, ids:List[str]|str='all', 
    json:dict={} # should be list of entries
):
    """ Prepare wave function population """

    try: # Parse arguments
        the_key='records'
        assert the_key in json.keys(), f"Expected key \'{the_key}\' in provided json objects."
        try:
            lots=[ Wave_Function(**x, blank=True) for x in json[the_key] ]
        except Exception as ex:
            raise Exception(f"Could not process argument of '{the_key}' as list of {Wave_Function} in provided json objects.")
        assert len(lots)>0, f"Did not provide any {the_key} entries!"
    except Exception as ex: my_exception(f"Problem in preparing wave base objects:", ex)

    # make the objects
    try:
        selected_ids=tracker.get_ids_for_table(Conformation, ids)

        new_wfn=[]
        for the_id in selected_ids:
            for lot in lots:
                kwargs=lot.model_dump()
                kwargs['conformation_id']=the_id
                new_wfn+=[Wave_Function(**kwargs ).model_dump()]

    except Exception as ex: my_exception(f"Problem in populationg conformations",ex)

    return tracker,new_wfn

def prep_compound_pop(
    tracker:pop_tracker, ids:None, 
    json:dict={},
)-> Tuple[pop_tracker,List[dict]]:

    try:
        def check_in_json(key):
            if not key in json.keys():
                raise Exception(f"Expected key \'{key}\' in provided json objects. Got keys: {json.keys()}")
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
        tracker:pop_tracker, ids:List[str]|str='all', 
        json:dict={}                  
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
