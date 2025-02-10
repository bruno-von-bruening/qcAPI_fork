## These functions will run on fastapi server
from . import *

def populate_functions(app, SessionDep): 
    def get_conformations(session):
        conformations=[ conf.model_dump() for conf in session.exec(select(QCRecord)).all()  ]
        return conformations
    def update(session,the_class, new_object, ids, force=False):
        """ """
        id=new_object.id
        found=session.get(the_class, id)
        
        if not isinstance(found, type(None)):
            # In case the old entry was not valid, lets try again
            assert 'converged' in found.__dict__.keys(), f"\'converged\' not a key of {the_class} but the existence is assumed in this funtion"
            if force or found.converged != 1:
                found.sqlmodel_update(new_object)
                session.add(found)
                session.commit()
                ids['replaced']+=1
            else:
                ids['omitted']+=1
        else:
            session.add(new_object)
            session.commit()
            ids['newly_inserted']+=1#ids['newly_inserted']+1
        return ids
    def gen_populate(session, property, force=False, method=None):
        ids={
            'omitted':0,
            'replaced':0,
            'newly_inserted':0,
        }
        # Get conformations as dict
        conformations=get_conformations(session)
        for conf in conformations:
            id=conf['id']

            if property in ['part']:
                # Create new object dependant on conformation id!
                if not method.upper() in ['LISA','MBIS']: raise Exception(f"Unkown method: {method.upper()}")
                method=method.upper()
                if not 'fchk_file' in conf.keys(): raise Exception(f"No key \'fchk_file\' in record {conf.keys()}")
                part=hirshfeld_partitioning(record_id=id, method=method, fchk_file=conf['fchk_file'])

                ids=update(session, hirshfeld_partitioning, part, ids, force=force)
            else:
                raise Exception(f"Cannot handle property yet: \'{property}\'")
        
        return ids
    def gen_populate_wrap(property, method, session, force=False):
        """ """
        try:
            if property in ['part']:
                if isinstance(method,type(None)):
                    raise Exception(f"Propertry {property} requires providing a method!")
            ids=gen_populate(session, property, force=force, method=method)
            message='Population Succesful'
            error=None
        except Exception as ex:
            raise HTTPException(215, detail=str(ex))
        return {'ids':ids,'return_message':message, 'advice':f"Have a look into the ids section (omitted would be entries that already have been converged by another job)"}
    #  @app.post("/populate/{property}")
    #  async def populate(
    #      property : str    ,
    #      session: SessionDep,
    #      force: bool = False,
    #  ):
    #      method=None
    #      json_return=gen_populate_wrap(property, method, session, force=force)
    #      return json_return
    #  @app.post("/populate/{property}/{method}")
    #  async def populate(
    #      property : str    ,
    #      method : str    ,
    #      session: SessionDep,
    #      force: bool = False,
    #  ):
    #      gen_populate_wrap(property, method, session, force=force)
    # @app.post("/populate/{property}/{method}/{basis}")
    # async def populate(
    #     basis: str,
    #     method: str,
    #     conformations: List[dict],
    #     session: SessionDep,
    #     force: bool = False,
    # ):
    #     try:
    #         ids = []
    #         for conformation in conformations:
    #             inchi, inchi_key, source, comments=[ conformation[x] for x in ['inchi','inchikey','source','comments'] ]
    #             compound_id=inchi_key

    #             elements=conformation['species']
    #             coordinates=conformation['coordinates']

    #             the_compound=session.get(Compound, compound_id)
    #             if isinstance(the_compound, type(None)):
    #                 new_compound=Compound(inchikey=compound_id, charge=0, multiplicity=1, elements=elements)
    #                 session.add(new_compound)
    #                 session.commit()

    #             new_conf=Conformation(compound_id=compound_id, source=source, comments=comments, elements=elements, coordinates=coordinates)
    #             session.add(new_conf)
    #             session.commit()
    #             
    #             ids.append(new_conf.id)
    #         session.commit()
    #         return {"message": "Data inserted successfully", "ids": ids}
    #     except Exception as ex:
    #         raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed to populate: {str(ex)}")
    
    def populate_conformation(session, conformations):
        ids = []
        for conformation in conformations:
            inchi, inchi_key, source, comments=[ conformation[x] for x in ['inchi','inchikey','source','comments'] ]
            compound_id=inchi_key

            elements=conformation['species']
            coordinates=conformation['coordinates']

            the_compound=session.get(Compound, compound_id)
            if isinstance(the_compound, type(None)):
                new_compound=Compound(inchikey=compound_id, charge=0, multiplicity=1, elements=elements)
                session.add(new_compound)
                session.commit()

            new_conf=Conformation(compound_id=compound_id, source=source, comments=comments, elements=elements, coordinates=coordinates)
            session.add(new_conf)
            session.commit()
            
            ids.append(new_conf.id)
        session.commit()
        return {"message": "Data inserted successfully", "ids": ids}

    def populate_wfn(session, method, basis, conformation_ids):
        ids=[]
        for conformation_id in conformation_ids:
            the_conformation=session.get(Conformation, conformation_id)
            if isinstance(the_conformation, type(None)):
                raise Exception(f"Did not find {Conformation.__name__} for id: {conformation_id}")
            
            new_wfn=Wave_Function(conformation_id=conformation_id, method=method, basis=basis, proctol=None)
            session.add(new_wfn)
            session.commit()
            ids.append(new_wfn.id)
        return {'message': "Data inserted succesfully", "ids": ids}
    
    def populate_part(session, method, wave_function_ids: str | List):
        ids=[]
        if isinstance(wave_function_ids, str):
            if wave_function_ids.lower()=='all':
                wave_function_ids=session.exec(select(Wave_Function.id).where(Wave_Function.converged==1)).all()
            else:
                raise Exception(f"Unkonw code for ids: {wave_function_ids}")
        elif isinstance(wave_function_ids, list):
            pass
        else:
            raise Exception(f"Unkown type {type(wave_function_ids)}")

        for id in wave_function_ids:
            the_wave_function=session.get(Wave_Function, id)

            new_part=Hirshfeld_Partitioning(wave_function_id=id, method=method)
            session.add(new_part)
            session.commit()
            ids.append(new_part.id)
        return {'message': "Data inserted succesfully", "ids": ids}
    
    def populate_wrapper(object, session, conformations=None, basis=None, method=None, ids=None):

        assert isinstance(object, str)
        object=object.lower()
        if object in ['conformation']:
            try:
                assert not isinstance(conformations, type(None)), f"Conformations is none!"
                return populate_conformation(session, conformations)
            except Exception as ex:
                raise Exception(f"Failed to populate: {str(ex)}")
        elif object in ['wfn', 'wave_function']:
            try:
                assert not isinstance(method, type(None))
                assert not isinstance(basis, type(None))
                assert not isinstance(ids, type(None))
                populate_wfn(session, method, basis, ids)
            except Exception as ex:
                raise Exception(f"Error: {ex}")
        elif object in ['part', 'partitioning']:
            try:
                assert not isinstance(method, type(None))
                assert not isinstance(ids, type(None))
                populate_part(session, method, ids)
            except Exception  as ex:
                raise Exception(f"Error: {ex}")

        else:
            raise Exception(f"Unkown object type: \'{object}\'")

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
            conf_key='conformations'
            ids_key='ids'
            if conf_key in json.keys():
                conformations=json[conf_key]
            else:
                conformations=None
            if ids_key in json.keys():
                ids=json[ids_key]
            else:
                ids=None
            return populate_wrapper(object, session, conformations=conformations, basis=basis, method=method, ids=ids)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in populating: {str(ex)}")
    
    return do_populate