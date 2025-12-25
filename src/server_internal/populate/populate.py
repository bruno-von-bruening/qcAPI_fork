## These functions will run on fastapi server
from . import *
from .util import pop_tracker
from enum import Enum

from fastapi import Query
from .body import generic_populate, gen_prepare_records

@val_call
def populate_wrapper(
        object, session, # for both wfn and part
        grid_pairs=None, # For grid
        ids: List[str|int]|Literal['all']|None=None, # For wfn, part, esp
        specs: dict={},
        json:dict={},
):
    try:
        # assert object is a string and make it lower case
        assert isinstance(object, str)
        model=get_object_for_tag(object)
    except Exception as ex: my_exception(f"Problem in argument of {populate_wrapper}:", ex)



    try:
        tracker=pop_tracker(session=session)
        tracker,prep_rec=gen_prepare_records(tracker, model, ids, specs=specs, json_data=json)
        tracker = generic_populate(tracker,model, prep_rec)

        return {'ids':tracker.id_tracker, 'counts':tracker.counter,'message':tracker.messanger.message}
    except Exception as ex: my_exception(f"Population did not work for object {model}:", ex)

import json as json_mod
def populate_functions(app, SessionDep): 

    @app.post("/populate/{object}")
    async def do_populate(
        object: str,
        session: SessionDep,
        ids: List[str|int|Literal['all']|None]=Query(None),
        specs: str='{}',
        json: dict={},
    ):
        try:
            if isinstance(ids, type(Query(None))):
                ids=ids.default
            elif not ids is None:
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
            specs=json_mod.loads(specs)
        except Exception as ex:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Problem in parsing specs parameter as json:\n{specs}\n {str(ex)}")

        
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

            if not len(specs)==0:
                kwargs.update(dict(specs=specs))


            # Actual population
            try:
                messages=populate_wrapper(object, session, **kwargs, json=json)
            except Exception as ex:
                raise my_exception(f"Problem in populating object of type \'{object}\'", ex)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in populating: {str(ex)}")
        return messages
    
    return do_populate