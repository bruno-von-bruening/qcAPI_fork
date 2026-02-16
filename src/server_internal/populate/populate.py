## These functions will run on fastapi server
from . import *
from .util import pop_tracker
from enum import Enum

from fastapi import Query
from .body import generic_populate, gen_prepare_records_switch

@val_call
def populate_main(
        object:SQLModelMetaclass,
        session:Session,
        ids: List[str|int]|Literal['all']='all',
        json:dict={},
):
    """ Populate a given objects """

    try:
        tracker=pop_tracker(session=session)
        tracker,prep_rec=gen_prepare_records_switch(tracker, object, ids, json_data=json)
        tracker = generic_populate(tracker, object, prep_rec)

        return {'ids':tracker.id_tracker, 'counts':tracker.counter,'message':tracker.messanger.message}
    except Exception as ex: my_exception(f"Population did not work for object {object}:", ex)

import json as json_mod
def populate_functions(app, SessionDep): 

    @app.post("/populate/{object}")
    async def do_populate(
        object: str,
        session: SessionDep,
        ids: List[str|int|Literal['all']|None]=Query(None),
        # specs: str='{}', # suspect this is not needed anymore
        json: dict={},
    ):
        # Cast ids to string if list of length 1 provided
        try:
            if isinstance(ids, type(Query(None))):
                ids='all' # ids.default
            elif ids is None:
                ids='all'
            else:
                if len(ids)==0:
                    ids='all'
                elif len(ids)==1:
                    if ids[0]=='all':
                        ids='all'
                    elif ids[0] is None:
                        ids='all'
                else:
                    assert not any( x=='all' for x in ids), f"Cannot combine 'all' with other ids in the list."
                    assert not any( x is None for x in ids), f"Cannot combine None with other ids in the list."
        except Exception as ex:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Problem in parsing ids parameter: {str(ex)}")

        # Actual population
        try:
            obj=get_object_for_tag(object)
            messages=populate_main(obj, session, ids=ids, json=json)
            return messages
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in populating object of type \'{object}\': {str(ex)}")
    
    return do_populate