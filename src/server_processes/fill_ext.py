from . import *

@validate_call
def get_previous_record_wrap(session, object, id: str|int):
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
    try:
        run_data_key='run_data'
        assert run_data_key in entry.keys(), f"Expected key \'{run_data_key}\' in entry!"
        run_data=entry[run_data_key]
        del entry[run_data_key]

        run_info_key='run_info'
        if run_info_key in entry.keys():
            del entry[run_info_key]

        return entry, run_data
    except Exception as ex: my_exception(f"Problem in processing run_data:", ex)

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
    old_record=get_previous_record_wrap(session, object, id)

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
    old_record=get_previous_record_wrap(session,the_object, id)

    run_data=entry['run_data']
    del entry['run_data']

    if entry['converged'] < 0:
        return {"message": "Record not processed. Ignoring."}
    elif entry['converged']!=0:
        if run_data is None:
            entry['converged']=0
        else:
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
        if stats_key in run_data.keys():
            stats=run_data[stats_key]
            if stats is not None:
                assert isinstance(stats, dict),  f"Expected dictinoary for stats, got {type(stats)}"
                stats.update({'id':id})
                stats=create_record(session, RHO_ESP_MAP_Stats, stats)
    
    new_record=the_object(**entry)
    return old_record, new_record

@val_call
def fill_part(session:Session, tracker:track_http_request, the_model:SQLModelMetaclass, entry:dict, sub_entries:dict):
    """  """

    # create enriched dictionary
    try:
        def get_model(tag):
            try:
                the_model=get_object_for_tag(tag)
                return the_model
            except Exception as ex: my_exception(f"Could not interprete {tag} as model", ex)
        
        mapper=dict(   )
        for k,v in sub_entries.items():
            the_model=get_model(k)
            the_key=the_model.__name__
            assert the_key not in mapper.keys(), f"Double assignment of entry (maps to {the_key})"
            if entry['method'].upper() in ['GDMA'] and the_key==ISA_Weights.__name__:
                assert v is None
            else:
                mapper.update({
                    the_key:dict(
                        entry=v,
                        the_model=the_model,
                    )
                })
    except Exception as ex: my_exception(f"Error in preparing sub entries", ex)
    
    ### CHECKS
    try:
        expected=dict(  (x['model'].__name__, x) for x in 
            [ dict(model=Distributed_Multipoles,  mandatory=True),
            dict(model=ISA_Weights, mandatory=( entry['method'].upper() not in ['GDMA'] ) ),
            dict(model=MOM_File, mandatory=False),
            ]
        )
        for k,v in mapper.items():
            assert k in expected.keys(), f"Key {k} not recognized"
        for k,v in expected.items():
            if entry['converged']==RecordStatus.converged:
                if v['mandatory']:
                    assert k in mapper.keys(), f"Expected key for {k}"
                    assert mapper[k] is  not None, f"Expected dictionary for {k} but is None"
                    assert isinstance(mapper[k] , dict), f"Expected dictionary for {k} but is {mapper[k]}"
    except Exception as ex: raise Exception(f"Problem during check of validity of provided sub entries", ex)
        #entry['converged']=0
        #tracker.add_error(f"Could not fill the record, {str(ex)}")
        #return entry

    ###
    try:
        for k,v in mapper.items():
            the_model=v['the_model']
            the_entry=v['entry']
            assert isinstance(the_entry, dict), f"Expected dictionary, got {the_entry}" 
            test=the_model(**the_entry)

        for k,v in mapper.items():
            the_model=v['the_model']
            the_entry=v['entry']
            create_record(session, the_model, the_entry, update_if_exists=True, commit=True)
    except Exception as ex: my_exception(f"Problem in populating records", ex)
    
    return tracker
    
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def fill_map_file(
    session,file_obj:SQLModelMetaclass, stats_obj:SQLModelMetaclass, entry:dict
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
    old_record=get_previous_record_wrap(session,the_object, id)

    converged=entry['converged']
    if converged==RecordStatus.converged:
        entry=fill_map_file(session, file_obj, stats_obj, entry)

    new_record=the_object(**entry)
    return old_record, new_record

@validate_call
def fill_espcmp(
    session, 
    entry:dict,
) -> Tuple[pdtc_sql_row, pdtc_sql_row]:
    the_object      = DMP_vs_RHO_ESP_Map
    file_obj        = DMP_vs_RHO_MAP_File
    stats_obj       = DMP_vs_RHO_MAP_Stats
    try:
        id=entry['id']
        old_record=get_previous_record_wrap(session,the_object, id)

        converged=entry['converged']
        if converged==RecordStatus.converged:
            entry=fill_map_file(session, file_obj, stats_obj, entry)

        new_record=the_object(**entry)
    except Exception as ex:
        raise Exception(f"Problem in executing {fill_espcmp}: {analyse_exception(ex)}")
    return old_record, new_record


