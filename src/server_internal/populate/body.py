from . import *
from .populate_ext import *
from .util import pop_tracker

from .prepare_populate import (
    prep_compound_pop,
    prep_wfn_pop,
    prep_conformation_pop,
    prep_molpol_pop,
)

@val_call
def gen_prepare_records_switch(
    tracker:pop_tracker, 
    model:SQLModelMetaclass, 
    ids:Union[List[str],Literal['all']], 
    json_data:dict={},
) -> Tuple[pop_tracker,List[dict]]:
    """ Pass the right arugments to the prepare function for UNIQUE_TAG"""
    if model==Compound:
        func=prep_compound_pop
    elif model==Conformation:
        func=prep_conformation_pop
    elif model==Wave_Function:
        func=prep_wfn_pop
    elif model==Molecular_Polarizability:
        func=prep_molpol_pop
    else: raise NotImplementedError(f"Did not implement prepare_records for model: {model.__name__}")

    try:
        return func(tracker=tracker, ids=ids, json=json_data)
    except Exception as ex: raise my_exception(f"Problem in preparing records for {model.__name__} with {func}:", ex)

@val_call
def generic_populate(
    tracker:pop_tracker,object: SQLModelMetaclass, records: List[dict | SQLModel],
) -> pop_tracker:
    

    def parse_records(records):
        if len(records)==0: return []# f"No records provided"
        recs_in_format=[]
        try:
            for rec_raw in records:
                if isinstance(rec_raw, SQLModel): rec_raw=rec_raw.model_dump()
                assert isinstance(rec_raw, dict), f"Provided record is not a dictionary!"
                rec=object(**rec_raw)
                recs_in_format+=[ rec ]
            # Check if already there
        except Exception as ex: my_exception(f"Problem in interpreting input as {object.__name__}",ex)
        return recs_in_format
    def identify_existing_records(records, id_tracker):
        try:
            keys=[getattr(c, prim_name) for c in records]
            confs_there=session.exec(select(object).where(get_primary_key(object).in_(keys))).all()
            for conf_ther in confs_there:
                id_tracker.add_omitted(getattr(conf_ther, prim_name))
            #compounds_there=[ inchikey for inchikey in [c.inchikey for c in compounds_in_format] if session.get(Compound, inchikey) is not None ]
            #assert len(compounds_there)==0, f"Compound to be popualted already existent"
        except Exception as ex: my_exception(f"Already exists:",ex)
        return id_tracker

    tracker.messanger.start_timing()
    session=tracker.session

    prim_name = get_primary_key_name(object)
    records_in_format=parse_records(records)
    tracker.id_tracker=identify_existing_records(records_in_format, tracker.id_tracker)

    for id in tracker.id_tracker.omitted:
        tracker.counter.already_there+=1

    try:
        from sqlalchemy import UniqueConstraint
        constraints=[]
        for constraint in object.__table__.constraints:
            if isinstance(constraint, UniqueConstraint):
                constraints+=tuple(constraint.columns.keys())
        if len(constraints)>0:
            # Get all existing combinations
            existing_combos=set()
            query=select( *( getattr(object, c) for c in [prim_name]+constraints ) )
            existing_rows=session.exec(query).all()
            for row in existing_rows:
                existing_combos.add( tuple( row[1:] ) )
            # Now check provided records if already there remove them from list
            to_be_removed=[]
            for i,c in enumerate(records_in_format):
                combo=tuple( getattr(c, col) for col in constraints )
                if combo in existing_combos:
                    id_key=getattr(c, prim_name)
                    if id_key is not None: tracker.id_tracker.add_omitted(id_key)
                    tracker.counter.already_there+=1
                    to_be_removed+=[i]
            # Remove in reverse order to not mess up indices
            records_in_format=[ c for i,c in enumerate(records_in_format) if not i in to_be_removed ]
    except Exception as ex:
        raise my_exception(f"Problem in checking unique constraints for {object.__name__}:", ex)
    
    # Insert
    try:
        for c in  records_in_format:
            id_key=getattr(c, prim_name)

            if id_key in tracker.id_tracker.succeeded:
                tracker.counter.doubly_requested+=1
            elif id_key in tracker.id_tracker.prerequisites_not_met:
                tracker.counter.prerequisites_not_met+=1
            elif id_key in tracker.id_tracker.omitted:
                tracker.counter.already_there+=1
            elif not id_key in tracker.id_tracker.omitted:
                the_rec=create_record(session, c, commit=True)[0]
                tracker.counter.populated +=1
                tracker.id_tracker.add_successful( getattr(the_rec,prim_name) )
            else:
                tracker.counter.unaccounted +=1

    except Exception as ex:
        tracker.counter.failed +=1
        raise Exception(analyse_exception(ex))


    # Give overview of this section
    def overview(tracker):
        di=vars(tracker.counter)
        to_print=dict(
            (k,v) for k,v in di.items() if v>0
        )
        if len(to_print)==0:
            to_print={"No changes":0}
            return [
                f"Nothing to populate for {object.__name__} (took {timing:.2f} [s])"
            ]
        else:
            max_len=max( len(k) for k in to_print.keys() )

            return [
                f"Populated {object.__name__} for {len(records_in_format)} provided entries (took {timing:.2f} [s]) (only mentioning where > 0):",
                #f"New entries: {tracker.counter.populated}",
                #f"Already existing entries: {tracker.counter.already_there}",
                #f"Prerequisites not met: {tracker.counter.prerequisites_not_met}",
            ]+[
                f" - {k:<{max_len}} : {v}" for k,v in to_print.items()
            ]
    timing=tracker.messanger.stop_timing(f"Populate {object.__name__}")
    tracker.messanger.add_message(overview(tracker))
    return tracker 
