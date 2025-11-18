from . import *
from .populate_ext import *

@val_call
def pop_molpol(
    session:Session, 
    wave_function_ids:List[str]|Literal['all']='all', # The ids of wave funciton
):
    """
    """
    the_object=Molecular_Polarizability
    try:
        tracker=pop_tracker(session=session)

        # Get available ids for objects
        ref_ids=tracker.get_ids_for_table(Wave_Function, ids='all')
        ancestor=Wave_Function


        new_objs=[]
        for the_id in ref_ids:
            assert isinstance(the_id, str), f"id is not a string: {the_id} ({type(the_id)})"
            new_objs+=[the_object(
                id=the_id, 
                approach='finite_field',
                code='psi4',
                tensor='dummy',
            )]


        # from sqlalchemy.orm import selectinload
        # stmt = (
        #     select(ancestor)
        #     .options(selectinload(ancestor.molecular_polarizability))  # fetch related items in one query
        # )
        # ret = session.exec(stmt)
        # raise Exception(ret.first() )#stmt.all())
        try: # Populate
            response=generic_populate(session, the_object, new_objs ,
                                       messanger=tracker.messanger, 
                                       count=tracker.counter,
                                        id_tracker=tracker.id_tracker)
        except Exception as ex: my_exception(f"Failure in populating {the_object.__name__}:", ex)
    except Exception as ex: raise my_exception(f"Problem in populating {the_object.__name__}", ex)