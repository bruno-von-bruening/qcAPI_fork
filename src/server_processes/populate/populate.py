## These functions will run on fastapi server
from . import *
from .util import pop_tracker
from enum import Enum

from .body import generic_populate, gen_prepare_records

# from .body import (
#     populate_conformation, populate_espcmp, populate_espdmp, populate_esprho, populate_isodens_surf, populate_part, populate_wfn, populate_group, 
#     populate_compound, pop_dispol, pop_molpol
# )

def populate_wrapper(object, session, # for both wfn and part
        grid_pairs=None, # For grid
        ids:List[str|int]|Literal['all']|None=None, # For wfn, part, esp
        json:dict={},
):
    try:
        # assert object is a string and make it lower case
        assert isinstance(object, str)
        model=get_object_for_tag(object)
    except Exception as ex: my_exception(f"Problem in argument of {populate_wrapper}:", ex)

    try:
        # def gen_populate(UNIQUE_TAG):
        #     try: 
        #         counter=None
        #         elif UNIQUE_TAG == NAME_CONF:
        #             counter = populate_conformation(session, conformations=json['conformations'])
        #         elif UNIQUE_TAG == NAME_WFN:
        #             pass
        #         elif UNIQUE_TAG == NAME_PART:
        #             assert not isinstance(method, type(None))
        #             assert not isinstance(ids, type(None))
        #             populate_part(session, method, ids, basis=basis)
        #         elif UNIQUE_TAG == NAME_IDSURF:
        #             assert not isinstance(grid_pairs, type(None)), f"Did not provide grid_pairs!"
        #             populate_isodens_surf(session, grid_pairs, ids)
        #         elif UNIQUE_TAG == NAME_ESPRHO:
        #             populate_esprho(session,ids)
        #         elif UNIQUE_TAG == NAME_ESPDMP:
        #             counter=populate_espdmp(session, surf_ids=ids, part_ids=None)
        #         elif NAME_ESPCMP    == UNIQUE_TAG:
        #             counter=populate_espcmp(session, espdmp_ids=None, espwfn_ids=None)
        #         elif NAME_GROUP     == UNIQUE_TAG:
        #             assert 'records' in json.keys(), f"Expected key \'records\' in provided json objects."
        #             counter=populate_group(session, groups=json['records'])
        #         elif NAME_DISPOL    == UNIQUE_TAG:
        #             counter=pop_dispol(session)
        #         elif NAME_MOLPOL    == UNIQUE_TAG:
        #             counter=pop_molpol(session)
        #         else:
        #             raise Exception(f"Did not implement function for UNIQUE_TAG type: \'{UNIQUE_TAG}\'")
        #         return counter
        #     except Exception as ex: my_exception(f"Error in populating for Tag \'{UNIQUE_TAG}\':", ex)
        tracker=pop_tracker(session=session)
        tracker,prep_rec=gen_prepare_records(tracker, model, ids, json_data=json)
        tracker = generic_populate(tracker,model, prep_rec)

        return {'ids':tracker.id_tracker, 'counts':tracker.counter,'message':tracker.messanger.message}
    except Exception as ex: my_exception(f"Population did not work for object {model}:", ex)

from fastapi import Query
def populate_functions(app, SessionDep): 

    @app.post("/populate/{object}")
    async def do_populate(
        object: str,
        session: SessionDep,
        ids: List[str|int|Literal['all']|None]=Query(None),
        json: dict={},
    ):
        try:
            if ids is None:
                ids=None
            else:
                if len(ids)==0:
                    ids=None
                elif len(ids)==1:
                    if ids[0]=='all':
                        ids='all'
                    elif ids[0] is None:
                        ids=None
                else:
                    assert not any( x=='all' for x in ids), f"Cannot combine 'all' with other ids in the list."
                    assert not any( x is None for x in ids), f"Cannot combine None with other ids in the list."
        except Exception as ex:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Problem in parsing ids parameter: {str(ex)}")
        
        try:
            try:
                conf_key='conformations'
                ids_key='ids'
                grid_key='grid_pairs'

                # kwargs={'basis':basis, 'method':method}
                kwargs={}

                if conf_key in json.keys(): 
                    pass
                    #kwargs.update(dict(
                    #    conformations=json[conf_key]
                    #))
                if ids_key in json.keys(): kwargs.update(dict(
                        ids=json[ids_key]))
                if grid_key in json.keys(): kwargs.update(dict(
                        grid_pairs=json[grid_key]))

                if ids is not None:
                    kwargs.update(dict(ids=ids))

                    
            except Exception as ex:
                my_exception(f"Problem in preparing the initial arguments for {populate_wrapper}", ex)

            try:
                messages=populate_wrapper(object, session, **kwargs, json=json)
            except Exception as ex:
                raise my_exception(f"Problem in populating object of type \'{object}\'", ex)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in populating: {str(ex)}")
        return messages
    
    return do_populate