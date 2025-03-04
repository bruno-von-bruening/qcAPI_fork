from . import *

def make_production_data(record, UNIQUE_NAME):
    try:
        if UNIQUE_NAME==NAME_WFN:
            production_data={}
        elif UNIQUE_NAME==NAME_PART:
            production_data={}
        elif UNIQUE_NAME==NAME_IDSURF:
            wfn_file=record.wave_function.wave_function_file
            fchk_file=wfn_file.full_path
            production_data={'fchk_file':fchk_file}
        elif NAME_ESPRHO==UNIQUE_NAME:
            wfn_file=record.wave_function.wave_function_file
            fchk_file=wfn_file.full_path
            surface_file=record.isodensity_surface.surface_file
            surface_file=surface_file.full_path
            production_data={'fchk_file':fchk_file, 'surface_file':surface_file}
        elif    NAME_ESPDMP==UNIQUE_NAME:
            moment_file=record.partitioning.moment_file.full_path
            surface_file=record.isodensity_surface.surface_file.full_path
            production_data={
                'moment_file':moment_file,
                'surface_file':surface_file
            }
        elif    NAME_ESPCMP == UNIQUE_NAME:
            rho_map_file=record.rho_map.map_file.full_path
            dmp_map_file=record.dmp_map.map_file.full_path
            production_data={
                'rho_map_file':rho_map_file,
                'dmp_map_file':dmp_map_file,
            }
        else:
            raise Exception(f"Cannot process property \'{UNIQUE_NAME}\'")
        return production_data
    except Exception as ex:
        raise Exception(f"Error in getting necessary related data for production of {UNIQUE_NAME}: {ex}")


@validate_call
def get_next_record(session, object: SQLModel, prop_args:my_dict={}):
    """ Get the next record to be processed
    if no unprocessed records are available break
    else create a worker
    propargs is a dict with key and target value
    - for_production: gathers all dependent information necessary to compute this property
    """

    
    ### GET THE RECORD
    # Check validity of the generic object
    keys=object.__dict__.keys()
    keys=[ k for k in keys if not k.startswith('_')]
    for mandatory_key in ['converged', 'timestamp']+list(prop_args.keys()):
        if mandatory_key not in keys: raise Exception(f"Key {mandatory_key} not in available keys ({keys}) or {object}")

    record=get_next_record_from_db(session, object, status=-1, prop_args=prop_args)

    return record

@validate_call
def create_worker(session,host_address, record):
    #   # in case no record was found start new threads for unfinished records (in case other workers are more powerful or a job is frozen)
    #   if isinstance(record, type(None)):
    #       record=filter(object, status=-2, prop_args=prop_args)

    #### Decide on continuation either break or create worker
    # Prepare new record and return it in case this fails send a signal
    try:
        # Create new worker
        timestamp = datetime.datetime.now().timestamp()
        worker = Worker(hostname=host_address, timestamp=timestamp)
        session.add(worker)

        # Update record
        record.timestamp = timestamp
        #   record.converged = -2 # Set this record to running (So it does not get executed doubly)
        session.add(record)
        session.commit()
        session.refresh(record)
        worker_id=worker.id
        return worker_id
    except Exception as ex:
        raise Exception(f"Error while creating worker ({create_worker}): {analyse_exception(ex)}")

def create_new_worker(session, request, property, method=None, for_production=True ):
        
    # Get the record and worker id for the next record
    try:
        UNIQUE_NAME=get_unique_tag(property)
        if UNIQUE_NAME==NAME_WFN:
            object=Wave_Function
            prop_args={}
        elif UNIQUE_NAME==NAME_PART:
            # Get result
            object=Hirshfeld_Partitioning
            prop_args={'method':method}
        elif    NAME_IDSURF == UNIQUE_NAME:
            object=IsoDens_Surface
            prop_args={}
        elif    NAME_ESPRHO == UNIQUE_NAME:
            object=RHO_ESP_Map
            prop_args={}
        elif    NAME_ESPDMP == UNIQUE_NAME:
            object=DMP_ESP_Map
            prop_args={}
        elif    NAME_ESPCMP == UNIQUE_NAME:
            object=DMP_vs_RHO_ESP_Map
            prop_args={}
        else:
            raise Exception(f"Cannot process property \'{property}\'")

        # Get the next record and if there is another record create a worker
        record= get_next_record(session, object, prop_args=prop_args)
        if record is None:
            worker_id=None
        else:
            host_address=f"{request.client.host}:{request.client.port}"
            worker_id=create_worker(session, host_address, record)
        return record, worker_id

    except Exception as ex:
        raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Error in retrieving record and worker id: {str(ex)}")
    
    # If record is empty there is nothing pending anymore, and we can continue!
    if isinstance(record, type(None)): 
        return record, worker_id

    # If production tag has been required enrich the folder
    if for_production:
        production_data=make_production_data(record, UNIQUE_NAME)
    record=record.model_dump()
    if for_production:
        record.update({'production_data':production_data})
    return record, worker_id