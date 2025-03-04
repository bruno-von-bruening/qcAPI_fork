from . import *

@validate_call
def get_previous_record_wrap(session, object, id: str|int)
    """ Get the previous record.
    If the record the calculation has been started with does not exist anymore then there is something wrong
    If it is already converged we will not overwrite it
    """
    old_record=get_prev_record(session, object, id)
    if old_record is None:
        raise HTTPException(status_code=HTTPStatus.CONFLICT, detail="Record does not exist")
    if old_record.converged == 1:
        raise HTTPException(status_code=HTTPStatus.NO_CONTENT, detail="Record already converged")
    return old_record

@validate_call
def get_run_data(entry:dict):
    run_data_key='run_data'
    assert run_data_key in entry.keys(), f"Expected key \'{run_data_key}\' in entry!"
    run_data=entry[run_data_key]
    assert isinstance(run_data, dict)
    del entry[run_data_key]

    run_info_key='run_info'
    if run_info_key in entry.keys():
        del entry[run_info_key]

    return entry, run_data

@validate_call
def get_from_run_data(data:dict, keys: List):
    found=[]
    for key in keys:
        if not key in data.keys(): 
            raise Exception(f"Excepted key \'{key}\' but found {data.keys()}")
        else:
            found.append(data[key])
    return found

def fill_idsurf(session, entry):
    object=IsoDens_Surface
    id=entry['id']
    old_record=get_prev_record_wrap(session, object, id)

    run_data=entry['run_data']
    surface_file=run_data['surface_file']
    surface_file.update({'id':id})
    create_record(session, IsoDens_Surf_File, surface_file)
    
    if entry['converged'] < 0:
        return {"message": "Record not processed. Ignoring."}
    else:
        pass
    new_record=object(**entry)
    return old_record, new_record
def fill_esprho(session, entry):
    the_object=RHO_ESP_Map
    file_obj=RHO_MAP_File

    id=entry['id']
    old_record=get_prev_record_wrap(session,the_object, id)

    run_data=entry['run_data']
    del entry['run_data']

    map_key='map_file'
    if map_key in run_data.keys():
        the_file=run_data[map_key]
        if isinstance(the_file, dict):
            the_file.update({'id':id})
            create_record(session, file_obj, the_file)
        else:
            entry['converged']=0
    else:
        entry['converged']=0
    
    stats_key='stats'
    if stats_key:
        stats=run_data[stats_key]
        if stats is not None:
            assert isinstance(stats, dict),  f"Expected dictinoary for stats, got {type(stats)}"
            stats.update({'id':id})
            stats=create_record(session, RHO_ESP_MAP_Stats, stats)
    
    if entry['converged'] < 0:
        return {"message": "Record not processed. Ignoring."}
    else:
        pass
    new_record=the_object(**entry)
    return old_record, new_record

def fill_part(session, entry):

    converged=entry['converged']

    # If record converged we expect subsidary files
    if converged==RecordStatus.converged:
        
        # Recover the data
        try:
            entry, run_data=get_run_data(entry)

            mul_key='multipoles'
            sol_key='solution'
            mom_fi_key='mom_file'
            multipoles, solution, mom_file=get_from_run_data(run_data, keys=[mul_key, sol_key, mom_fi_key])
        except Exception as ex:
            raise Exception(f"Could not recover data from provided entry: {analyse_exception(ex)}")
        

        # Update the multipoles 
        if not isinstance(multipoles, type(None)):
            try:
                multipoles.update({'id':entry['id']})
                mul=Distributed_Multipoles(**multipoles)
                session.add(mul)
                session.commit()
            except Exception as ex:
                raise Exception(f"Could not create multipoles: {analyse_exception(ex)}")
        else:
            if converged==1:
                raise Exception(f"No multipoles provided although converged!")
        # Update the multipole file
        if not isinstance(mom_file, type(None)):
            assert isinstance(mom_file, dict), f"Expected moment file in dictionary format, got: {type(mom_file)} {mom_file}"
            try:
                mom_file.update({'id':entry['id']})
                create_record(session, MOM_File, mom_file)
            except Exception as ex:
                raise Exception(f"Could not create multipole file: {analyse_exception(ex)}")
        else:
            if converged==1:
                raise Exception(f"No moment file provided although converged!")
        
        # Update the soltuions
        if not isinstance(solution, type(None)):
            try:
                solution.update({'id':entry['id']})
                sol=ISA_Weights(**solution)
                session.add(sol)
                session.commit()
            except Exception as ex:
                raise Exception(f"Could not create ISA_weights: {analyse_exception(ex)}")
        else:
            if converged==1:
                raise Exception(f"No multipoles provided although converged!")
        
    # Update the partitioning
    the_object=Hirshfeld_Partitioning
    try:
        if entry['converged'] < 0:
            return {"message": "Partitioning not processed. Ignoring."}

        id=entry['id']
        prev_part=get_prev_record_wrap(session, the_object, id)
        new_record=the_object(**entry)
        return prev_part, new_record
    except Exception as ex:
        raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Error in updating {the_object.__name__}: {ex}")



    
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def fill_map_file(
    session,file_obj:sqlmodel_cl_meta, stats_obj:sqlmodel_cl_meta, entry:dict
) -> dict:

    def inner_func():
        try:
            run_data=entry['run_data']
            del entry['run_data']
                # Process input
        except Exception as ex:
            raise Exception(f"Did not find run_data in entry: {ex}")

        if run_data is None:
            raise Exception(f"Could not fill: run_data is None")
        else:
            try:
                parent_id=entry['id']
                map_file=run_data['map_file']
                stats=run_data['stats']
            except Exception as ex:
                raise Exception(f"Provided run_data appears to be incomplete ({run_data}): {ex}")

        # Try to update map file
        try:
            assert isinstance(map_file, dict),f"Expected dictionary for map file"
            map_file.update({'id':parent_id})
            try:
                file=create_record(session, file_obj, map_file)
            except Exception as ex:
                raise Exception(f"Could not implement object {file_obj.__name__} with {map_file}:\nerror={ex}")
        except Exception as ex:
            raise Exception(f"Could not process creation of {file_obj.__name__}: {ex}")

        # Try to update statistics
        try:
            if stats is not None:
                assert isinstance(stats, dict),  f"Expected dictinoary for stats, got {type(stats)}"
                stats.update({'id':parent_id})
                stats=create_record(session, stats_obj, stats)
        except Exception as ex:
            entry['warnings'].append(f"Could not process creation of {stats_obj.__name__}: {ex}")
        return entry

    try:
        entry=inner_func()
    except Exception as ex:
        entry['errors']=json.dumps( json.loads(entry['errors'])+[str(ex)])
        entry.update({'converged':0})
    return entry


@validate_call
def fill_espdmp(session, entry: dict):
    the_object      = DMP_ESP_Map
    file_obj        = DMP_MAP_File
    stats_obj       = DMP_ESP_MAP_Stats

    id=entry['id']
    old_record=get_prev_record_wrap(session,the_object, id)

    converged=entry['converged']
    if converged==RecordStatus.converged:
        entry=fill_map_file(session, file_obj, stats_obj, entry)

    new_record=the_object(**entry)
    return old_record, new_record

@validate_call
def fill_espcmp(
    session, 
    entry:dict,
) -> Tuple[sqlmodel, sqlmodel]:
    the_object      = DMP_vs_RHO_ESP_Map
    file_obj        = DMP_vs_RHO_MAP_File
    stats_obj       = DMP_vs_RHO_MAP_Stats
    try:
        id=entry['id']
        old_record=get_prev_record_wrap(session,the_object, id)

        converged=entry['converged']
        if converged==RecordStatus.converged:
            entry=fill_map_file(session, file_obj, stats_obj, entry)

        new_record=the_object(**entry)
    except Exception as ex:
        raise Exception(f"Problem in executing {fill_espcmp}: {analyse_exception(ex)}")
    return old_record, new_record
