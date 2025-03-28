from . import *
from .util import get_ids_for_object, message_tracker, counter, get_rows, track_ids

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
                        the_compound=create_record(session, Compound, compound)[0]
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
            conf=create_record(session, Conformation, new_conf)[0]

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

@validate_call
def populate_part(session, method, wave_function_ids: str | List, basis:str|None=None):
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
        old_part=session.get(Wave_Function, id)

        new_part=Hirshfeld_Partitioning(wave_function_id=id, method=method, basis=basis)
        session.add(new_part)
        session.commit()
        ids.append(new_part.id)
    return {'message': "Data inserted succesfully", "ids": ids}

def populate_isodens_surf(session, grid_pairs, ids=None):
    """Filtering inside this function does not make so much sense --> should be put in a utility anywar, 
    take the ids and load them """
    try:
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
    except Exception as ex: my_exception(f"Failure in {populate_isodens_surf}:", ex)

def populate_esprho(session, surf_ids, wfn_ids=None):
    """ """
    try: 
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
        create_record(session, RHO_ESP_Map, dicts, commit=True)
    except Exception as ex: my_exception(f"Failure of {populate_esprho}:", ex)

def populate_espdmp(session, surf_ids, part_ids, method=None):
    """ """
    messager=message_tracker()
    try:
        if surf_ids is None:
            # Get wfn
            wfn_ids=get_ids_for_object(session, Wave_Function)
            messager.add_message(
                f"Since no surface ids where provided this routine will find them over the {Wave_Function.__name__} object."+
                f"Found {len(wfn_ids)} {Wave_Function.__name__}"
            )
        else:
            wfn_ids=None
            assert all([ session.get(IsoDens_Surface,x) is not None for x  in surf_ids])
            message+=[f"You provided {len(surf_ids)} surface ids which are valid"]
    except Exception as ex: my_exception(f"Problem in getting input", ex)


    # Get two objects that are related over the wave function: surface and maps
    def get_crosses(wfn_ids):
        crosses=[]
        # For every wave function get the product between all of its multipoles and surfaces!
        for wfn_id in wfn_ids:
            surf_ids=get_rows(session, IsoDens_Surface, selection=['primary_key','converged'], filter_args={'wave_function_id':wfn_id})
            part_ids=get_rows(session, Hirshfeld_Partitioning, selection=['primary_key','converged'], filter_args={'wave_function_id':wfn_id})
            for surf_id, surf_converged in surf_ids:
                for part_id,part_converged in part_ids:
                    cross={'surf_id':surf_id,'part_id':part_id, 'surf_converged':surf_converged,'part_converged':part_converged}
                    crosses.append(cross)
        return crosses
    try:
        if not wfn_ids is  None:
            crosses=get_crosses(wfn_ids)
        else:
            for surf_id in surf_ids:
                dmp_ids= [ [x.id for x in session.get(IsoDens_Surface, id).wave_function.hirshfeld_partitionings] for id in surf_ids ]
    except Exception as ex:
        raise Exception(f"Error in generation pairs between surfaces and partitionings for multipolar esp: {analyse_exception(ex)}")

    try:
        the_count=counter()
    except Exception as ex:
        raise Exception(f"Error in counter initializaiton of counter: {analyse_exception(ex)}")

    try:
        DMP=DMP_ESP_Map
        keys=['partitioning_id','surface_id','ranks']
        comps=get_rows(session, DMP_ESP_Map, selection=keys)
        comps=[ dict([(k,v) for k,v in zip(keys,c)]) for c in comps]


        there=[]
        for cross in crosses:
            surf_id,part_id,surf_converged,part_converged=[ cross[k] for k in ['surf_id','part_id','surf_converged','part_converged']]
            if part_converged!=1 or surf_converged!=1:
                the_count.prerequisites_not_met+=1
                continue
            else:
                new_di={'partitioning_id':part_id, 'surface_id': surf_id}
                def check_equal(comps, new_di):
                    # in the database I acciedetally read an integer in as a string
                    return [ c for c in comps if all([str(c[k])==str(new_di[k]) for k in new_di.keys() ])]
                dics=check_equal(comps, new_di)
                for max_rank in range(5):
                    new_di.update({'ranks':f"max{max_rank}"})
                    found=check_equal(dics, new_di)

                    if len(found)>0:
                        already_there=True
                    else:
                        already_there=False

                
                    #query=select(DMP_ESP_Map)
                    #for k,v in new_di.items():
                    #    query=query.where(getattr(DMP_ESP_Map, k)==v)
                    ## Check if there is already a map with the same content
                    #already_there=any([x.converged for x in session.exec(query).all()])
                
                    if not already_there:
                        create_record(session, DMP_ESP_Map, new_di, commit=False)
                    there.append(already_there)
        messager.add_message([
            f"Records that where already there: {len([t for t in there if t])}",
            f"Records created: {len([t for t in there if not t])}"
        ])
        session.commit()
        return {'message':messager.message}
    except Exception as ex:
        raise Exception(f"Error in generating object: {analyse_exception(ex)}")

@validate_call
def populate_espcmp(session, espdmp_ids:List[int|str]|None=None, espwfn_ids:List[int|str]|None=None):
    """
    
    """
    messanger=message_tracker()

    if espdmp_ids is not None or espwfn_ids is not None:
        raise Exception(f"Did not implement active reading of the method yet")

    object  = DMP_vs_RHO_ESP_Map
    surf    = IsoDens_Surface
    dmp_tab = DMP_ESP_Map
    rho_tab = RHO_ESP_Map
    dmp_vs_rho_tab = DMP_vs_RHO_ESP_Map

    # Get surface ids
    try:    
        surf_ids=get_ids_for_object(session, surf)
        messanger.add_message(f"Found {len(surf_ids)} surfaces that will be queried.")
    except Exception as ex: my_exception(f"Problem in getting information from {surf.__name__}", ex)

    # Get crosses
    try:
        start=time.time()
        crosses=[]
        for surf_id in surf_ids:
            dmp_ids = get_rows(session, dmp_tab, selection=['primary_key','converged'] , filter_args={'surface_id':surf_id})
            rho_ids = get_rows(session, rho_tab, selection=['primary_key','converged'] , filter_args={'surface_id':surf_id})

            for dmp_id, dmp_converged in dmp_ids:
                for rho_id, rho_converged in rho_ids:
                    crosses.append([surf_id, dmp_id, rho_id, dmp_converged, rho_converged])
        messanger.add_message(f"Found {len(crosses)} combinations of Multipolar and Density Maps")
    except Exception as ex: my_exception(f"Failure in generating the pairs of previos objects:", ex)

    # Will
    try:
        start=time.time()
        cmp_keys=[get_primary_key_name(dmp_vs_rho_tab),'dmp_map_id','rho_map_id','converged']
        existing_cmp=get_rows(session, dmp_vs_rho_tab, selection=cmp_keys)

        rows_there=[]
        rows_failed=[]
        rows_new=[]
        rows_no_dmp=[]
        rows_no_rho=[]
        rows_no_both=[]
        for  cross in crosses:
            surf_id,dmp_id, rho_id, dmp_converged, rho_converged = cross

            if not dmp_converged==1 and not rho_converged==1:
                rows_no_both.append(cross)
            elif not dmp_converged==1:
                rows_no_dmp.append(cross)
            elif not rho_converged==1:
                rows_no_rho.append(cross)
            else:
                found=[ x  for x in existing_cmp if x[1]==dmp_id and x[2] ==rho_id]
                already_there= ( len(found)>0 )
                if already_there:
                    if any([x[3]==1 for x in found]):
                        rows_there.append(cross)
                    else:
                        rows_failed.append(cross)
                else:
                    rows_new.append(cross)

        messanger.add_message([
                f"Filtered out existing entries (took {time.time()-start:.2f} [s]):",
                f"Prerequisites not met: no_dmp={len(rows_no_dmp)}, no_rho={len(rows_no_rho)}, both={len(rows_no_both)}",
                f"Already existing and converged: {len(rows_there)}",
                f"Already existing but not converged: {len(rows_failed)}",
                f"New combinations: {len(rows_new)}",
        ])
    except Exception as ex: my_exception(f"Failure in generating the pairs of previos objects {messanger.message}:", ex)

    # Go through the crosses
    try:
        new_dicts=[ dict(dmp_map_id=dmp_id, rho_map_id=rho_id)
            for surf_id,dmp_id,rho_id, dmp_converged, rho_converged in rows_new+rows_failed ]
        create_record(session, DMP_vs_RHO_ESP_Map, new_dicts, commit=True)
    except Exception as ex: my_exception(f"Error creating rows for table {object.__name__}:\n {messanger.message}",ex)

    return {'message': messanger.message}

@validate_call
def populate_group(session, groups:List[dict]):
    messanger=message_tracker()
    try:
        messanger.add_message(f"Provided {len(groups)} groups")
        from qc_groups.groups import node
        class node(node):
            def gen_group(self):
                return Group(id=self.id, name=self.name)
            def gen_group_to_groups(self):
                return [ Group_to_Group(lower_group_id=x, upper_group_id=self.id) for x in self.lower ]
            def gen_compound_to_group(self):
                return [Compound_to_Group(compound_id=key, group_id=self.id) for key in self.leaves]

        counter={'Groups':0, 'Group_to_Group':0,'Compound_to_Group':0}
        for group in groups:
            try:
                the_node=node(**group)
                the_group=the_node.gen_group()
                g_to_g=the_node.gen_group_to_groups()
                comp_to_group=the_node.gen_compound_to_group()
                session.add(the_group)        
                counter['Groups']+=1
                [ session.add(x) for x in g_to_g]
                counter['Group_to_Group']+=len(g_to_g)
                [ session.add(x) for x in comp_to_group]
                counter['Compound_to_Group']+=len(comp_to_group)
                session.commit()
            except Exception as ex: my_exception(f"Could not create group for {group}:", ex)
        tag=' '.join([ '\n'+4*' '+f"{k:<15} : {v}" for k,v in counter.items() ])
        messanger.add_message(f"Following objects were updated succefully:{tag}")
    except Exception as ex: my_exception(f"Problem in populating Groups:", ex)

    return {'message':messanger.message}