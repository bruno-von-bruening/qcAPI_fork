## These functions will run on fastapi server
from . import *
from enum import Enum

from .populate_ext import populate_conformation, populate_espcmp, populate_espdmp, populate_esprho, populate_isodens_surf, populate_part, populate_wfn, populate_group

def populate_wrapper(object, session, conformations=None, 
        basis=None, method=None, # for both wfn and part
        grid_pairs=None, # For grid
        ids=None,
        json:dict={},
):
    try:
        # assert object is a string and make it lower case
        assert isinstance(object, str)
        object=object.lower()

        UNIQUE_TAG=get_unique_tag(object)
    except Exception as ex: my_exception(f"Problem in argument of {populate_wrapper}:", ex)

    try:
        def gen_populate(UNIQUE_TAG):
            try: 
                counter=None
                if UNIQUE_TAG == NAME_CONF:
                    counter = populate_conformation(session, conformations)
                elif UNIQUE_TAG == NAME_WFN:
                    assert not isinstance(method, type(None))
                    assert not isinstance(basis, type(None))
                    assert not isinstance(ids, type(None))
                    populate_wfn(session, method, basis, ids)
                elif UNIQUE_TAG == NAME_PART:
                    assert not isinstance(method, type(None))
                    assert not isinstance(ids, type(None))
                    populate_part(session, method, ids)
                elif UNIQUE_TAG == NAME_IDSURF:
                    assert not isinstance(grid_pairs, type(None)), f"Did not provide grid_pairs!"
                    populate_isodens_surf(session, grid_pairs, ids)
                elif UNIQUE_TAG == NAME_ESPRHO:
                    populate_esprho(session,ids)
                elif UNIQUE_TAG == NAME_ESPDMP:
                    counter=populate_espdmp(session, surf_ids=ids, part_ids=None)
                elif NAME_ESPCMP    == UNIQUE_TAG:
                    counter=populate_espcmp(session, espdmp_ids=None, espwfn_ids=None)
                elif NAME_GROUP     == UNIQUE_TAG:
                    assert 'records' in json.keys(), f"Expected key \'records\' in provided json objects."
                    counter=populate_group(session, groups=json['records'])
                else:
                    raise Exception(f"Did not implement function for UNIQUE_TAG type: \'{UNIQUE_TAG}\'")
                return counter
            except Exception as ex: my_exception(f"Error in populating for Tag \'{UNIQUE_TAG}\':", ex)
        return gen_populate(UNIQUE_TAG)
    except Exception as ex:
        raise Exception(f"Population did not work for object {UNIQUE_TAG}: {analyse_exception(ex)}")


def populate_functions(app, SessionDep): 

    @app.post("/populate/{object}")
    async def do_populate(
        object: str,
        session: SessionDep,
        basis: str | None =None,
        method: str | None = None,
        json: dict={},
        #conformations: List[dict]|None, #|None,# List[dict]=None,
        #ids: List[int]|List[float]|None,
        #conformations: List[dict]|None =None,
    ):
        try:
            try:
                conf_key='conformations'
                ids_key='ids'
                grid_key='grid_pairs'

                kwargs={'basis':basis, 'method':method}

                if conf_key in json.keys(): kwargs.update(dict(
                        conformations=json[conf_key]))
                if ids_key in json.keys(): kwargs.update(dict(
                        ids=json[ids_key]))
                if grid_key in json.keys(): kwargs.update(dict(
                        grid_pairs=json[grid_key]))
            except Exception as ex:
                raise Exception(f"Problem in preparing the initial arguments for {populate_wrapper}")

            return populate_wrapper(object, session, **kwargs, json=json)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in populating: {str(ex)}")
    
    return do_populate