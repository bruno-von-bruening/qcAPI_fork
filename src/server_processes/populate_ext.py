from . import *

class counter():
    requested   = 0
    prerequisites_not_met=0
    populated    = 0
    failed = 0

class track_ids(BaseModel):
    succeeded: List[int|str] = []
    failed: List[int|str]= []
    @validate_call
    def add_successful(self, item: int|str ):
        self.succeeded+=[ item ]
    @validate_call
    def add_failed(self, item:int|str ):
        self.failed+=[ item ]


def get_ids_for_object(session, sql_table):
    convergd_wave_functions=filter_db(session,sql_table, filter_args={'converged':1})
    ids=[ x.id for x in convergd_wave_functions]
    return ids

@validate_call
def make_object(session, the_object, dicts:List[dict]|dict):
    if isinstance(dicts, dict):
        dicts=[dicts]
    try:
        objects=[]
        for dic in dicts:
            try:
                obj=the_object(**dic)
                session.add(obj)
                objects.append(obj)
                session.commit()
            except Exception as ex:
                raise Exception(f"Could not fill {the_object.__name__} with: {dic}:\n{analyse_exception(ex)}")
        return objects
    except Exception as ex:
        raise Exception(analyse_exception(ex))

def populate_esprho(session, surf_ids, wfn_ids=None):
    """ """
    if isinstance(surf_ids, type(None)):
        surf_ids=get_ids_for_object(session,IsoDens_Surface)
    if not wfn_ids is None: raise Exception(f"Supplied wave function ids but their handling is not yet implemented.")
    wfn_ids= [ session.get(IsoDens_Surface, id).wave_function.id for id in surf_ids ]
    
    assert len(surf_ids)==len(wfn_ids)
    dicts=[]
    for surf_id,wfn_id in zip(surf_ids, wfn_ids):
        dicts.append(dict(
            wave_function_id=wfn_id, 
            surface_id=surf_id,
            density_grid='insane',
        ))
    # ids not yet implemented in function
    ids=make_object(session, RHO_ESP_Map, dicts)

def populate_espdmp(session, surf_ids, part_ids, method=None):
    """ """
    try:
        if isinstance(surf_ids, type(None)):
            surf_ids=get_ids_for_object(session,IsoDens_Surface)
        if not isinstance(part_ids, type(None)): raise Exception(f"Supplied wave function ids but their handling is not yet implemented.")
        dmp_ids= [ [x.id for x in session.get(IsoDens_Surface, id).wave_function.hirshfeld_partitionings] for id in surf_ids ]
    except Exception as ex:
        raise Exception(f"Error in id generation: {analyse_exception(ex)}")

    try:
        the_count=counter()
    except Exception as ex:
        raise Exception(f"Error in counter initializaiton: {analyse_exception(ex)}")

    try:    
        for surf_id, the_dmp_ids in zip(surf_ids, dmp_ids):
            # If no multipoles are there then skip
            if len(the_dmp_ids)<1:
                the_count.prerequisites_not_met+=1
                continue
            
            for part_id in the_dmp_ids:
                if session.get(Hirshfeld_Partitioning, part_id).converged==1:
                    for max_rank in range(5):
                        new_di={'partitioning_id':part_id, 'surface_id': surf_id, 'ranks':f"max{max_rank}"}
                        create_record(session, DMP_ESP_Map, new_di)


    except Exception as ex:
        raise Exception(f"Error in generating object: {analyse_exception(ex)}")



        

    




# def get_conformations(session):
#     conformations=[ conf.model_dump() for conf in session.exec(select(QCRecord)).all()  ]
#     return conformations
# def update(session,the_class, new_object, ids, force=False):
#     """ """
#     id=new_object.id
#     found=session.get(the_class, id)
#     
#     if not isinstance(found, type(None)):
#         # In case the old entry was not valid, lets try again
#         assert 'converged' in found.__dict__.keys(), f"\'converged\' not a key of {the_class} but the existence is assumed in this funtion"
#         if force or found.converged != 1:
#             found.sqlmodel_update(new_object)
#             session.add(found)
#             session.commit()
#             ids['replaced']+=1
#         else:
#             ids['omitted']+=1
#     else:
#         session.add(new_object)
#         session.commit()
#         ids['newly_inserted']+=1#ids['newly_inserted']+1
#     return ids
# def gen_populate(session, property, force=False, method=None):
#     ids={
#         'omitted':0,
#         'replaced':0,
#         'newly_inserted':0,
#     }
#     # Get conformations as dict
#     conformations=get_conformations(session)
#     for conf in conformations:
#         id=conf['id']

#         if property in ['part']:
#             # Create new object dependant on conformation id!
#             if not method.upper() in ['LISA','MBIS']: raise Exception(f"Unkown method: {method.upper()}")
#             method=method.upper()
#             if not 'fchk_file' in conf.keys(): raise Exception(f"No key \'fchk_file\' in record {conf.keys()}")
#             part=hirshfeld_partitioning(record_id=id, method=method, fchk_file=conf['fchk_file'])

#             ids=update(session, hirshfeld_partitioning, part, ids, force=force)
#         else:
#             raise Exception(f"Cannot handle property yet: \'{property}\'")
#     
#     return ids
#def gen_populate_wrap(property, method, session, force=False):
#    """ """
#    try:
#        if property in ['part']:
#            if isinstance(method,type(None)):
#                raise Exception(f"Propertry {property} requires providing a method!")
#        ids=gen_populate(session, property, force=force, method=method)
#        message='Population Succesful'
#        error=None
#    except Exception as ex:
#        raise HTTPException(215, detail=str(ex))
#    return {'ids':ids,'return_message':message, 'advice':f"Have a look into the ids section (omitted would be entries that already have been converged by another job)"}
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
    count=counter()
    id_tracker=track_ids()
    for conformation in conformations:
        try:
            def make_compound(conformation):
                try:
                    key='compound'
                    compound=conformation[key]
                    del conformation[key]

                    #inchi, inchi_key, source, comments=[ compound[x] for x in ['inchi','inchikey','source','comments'] ]
                    compound.update({'elements':conformation['species']})

                    the_compound=session.get(Compound, compound['inchikey'])
                    if the_compound is None:
                        the_compound=make_object(session, Compound, compound)[0]
                    else:
                        raise Exception(f"Unexpectdelty found object of type {Compound.__name__} for"+
                                f" key=\"{compound['inchikey']}\": Cannot overwrite/update object yet.")
                except Exception as ex:
                    raise Exception(analyse_exception(ex))

                return the_compound

            comp=make_compound(conformation)
            compound_id=comp.inchikey
            elements=conformation['species']

            # make conformation
            elements=conformation['species']
            coordinates=conformation['coordinates']
            new_conf=dict(compound_id=compound_id, elements=elements, coordinates=coordinates)
            conf=make_object(session, Conformation, new_conf)[0]

            count.populated +=1
            id_tracker.add_successful(conf.id)
        except Exception as ex:
            raise Exception(analyse_exception(ex))
            count.failed +=1
    return {'ids':id_tracker, 'counts':count}
        

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

def populate_isodens_surf(session, grid_pairs, ids=None):
    """Filtering inside this function does not make so much sense --> should be put in a utility anywar, 
    take the ids and load them """
    if isinstance(ids, type(None)):
        ids=get_ids_for_object(session,Wave_Function)
    assert isinstance(grid_pairs, (list,np.ndarray)), f"Expected list type"
    assert isinstance(grid_pairs[0], (tuple,list)), f"Expexcted list of tuples or lists, got {type(grid_pairs[0])}"
    
    for wfn_id in ids:
        for isod, spac in grid_pairs:
            surf=IsoDens_Surface(wave_function_id=wfn_id, iso_density=isod, spacing=spac, algorithm='marching_cubes',
                    num_vertices=-1, num_faces=-1)
            session.add(surf)
    session.commit()

def populate_espcmp(session, espdmp_ids=None, espwfn_ids=None):
    if espdmp_ids is not None or espwfn_ids is not None:
        raise Exception(f"Did not implement active reading of the method yet")
    
    surf_ids=get_ids_for_object(session, IsoDens_Surface)
    object=DMP_vs_RHO_ESP_Map

    try:
        fill_it=[]
        for id in surf_ids:
            surface=session.get(IsoDens_Surface, id)
            dmp_maps=surface.esp_dmp_maps
            rho_maps=surface.esp_rho_maps

            if dmp_maps!=[] and rho_maps!=[]:
                for rho_map in rho_maps: 
                    for dmp_map in dmp_maps:
                        fill_it.append({'dmp':dmp_map, 'rho':rho_map})
    except Exception as ex:
        raise Exception(f"Error in Preparing data required for object of type {object.__name__}: {ex}")

    try:
        # Fill the values:
        for pair in fill_it:
            dmp_map=pair['dmp']            
            rho_map=pair['rho']
            comp=object(dmp_map_id=dmp_map.id, rho_map_id=rho_map.id)
            create_record(session, object, comp)
    except Exception as ex:
        raise Exception(f"Error in creating object of type {object.__name__}: {ex}")
