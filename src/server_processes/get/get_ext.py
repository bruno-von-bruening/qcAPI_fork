from . import *
from data_base.utils import get_object_for_tag

class return_data(myBaseModel):
    worker_id: str
    record_type: str
    record: SQLModel|List[SQLModel]
    sub_entries: Dict[str, SQLModel] = {}
    primary_keys: Dict[str, str] = {}

    def model_dump(self, *args, **kwargs):
        mod = super().model_dump(*args, **kwargs, exclude=['record', 'sub_entries'])
        mod['record'] = self.record.model_dump()
        mod['sub_entries'] = {
            k: v.model_dump() for k, v in self.sub_entries.items()
        }
        return mod

    def __init__(self, *args, **kwargs):
        assert 'record' in kwargs, "expected 'record' in keys"
        # Cast record to correct SQLModel type
        if isinstance(kwargs.get('record_type'), str):
            model = get_object_for_tag(kwargs['record_type'])
            if not isinstance(kwargs['record'], model):
                kwargs['record'] = model(**kwargs['record']) if isinstance(kwargs['record'], dict) else kwargs['record']
        # Cast sub_entries to correct SQLModel types
        if 'sub_entries' in kwargs and isinstance(kwargs['sub_entries'], dict):
            casted_sub_entries = {}
            for k, v in kwargs['sub_entries'].items():
                sub_model = get_object_for_tag(k)
                casted_sub_entries[k] = sub_model(**v) if isinstance(v, dict) and not isinstance(v, sub_model) else v
            kwargs['sub_entries'] = casted_sub_entries
        super().__init__(*args, **kwargs)

def get_objects(session, the_object, filters: dict={}):
    # Get all tables of the given type
    try:
        results= filter_db(session, object=the_object, filter_args=filters)
        return results
    except Exception as ex:
        message=f"Could not get the entries for property {the_object.__name__}: {ex}"
        raise Exception(message)
from data_base.database_declaration import Molecular_Polarizability, Compound, Conformation

@val_call
def make_production_data(data:return_data)->return_data:
    try:
        def ident(one:SQLModel, two:SQLModelMetaclass):
            return type(one).__name__==two.__name__
        
        if ident(data.record, Molecular_Polarizability):
            wfn=data.record.wave_function
            conf=wfn.conformation
            comp=conf.compound
            data.sub_entries.update({
                Wave_Function.__name__  : wfn,
                Compound.__name__       :comp,
                Conformation.__name__   :conf,
            })
        #elif UNIQUE_NAME==NAME_WFN:
        #    production_data={}
        #elif UNIQUE_NAME==NAME_PART:
        #    production_data={}
        #elif UNIQUE_NAME==NAME_IDSURF:
        #    wfn_file=record.wave_function.wave_function_file
        #    fchk_file=wfn_file.full_path
        #    production_data={'fchk_file':fchk_file}
        #elif NAME_ESPRHO==UNIQUE_NAME:
        #    wfn_file=record.wave_function.wave_function_file
        #    fchk_file=wfn_file.full_path
        #    surface_file=record.isodensity_surface.surface_file
        #    surface_file=surface_file.full_path
        #    production_data={'fchk_file':fchk_file, 'surface_file':surface_file}
        #elif    NAME_ESPDMP==UNIQUE_NAME:
        #    moment_file=record.partitioning.moment_file.full_path
        #    surface_file=record.isodensity_surface.surface_file.full_path
        #    production_data={
        #        'moment_file':moment_file,
        #        'surface_file':surface_file
        #    }
        #elif    NAME_ESPCMP == UNIQUE_NAME:
        #    rho_map_file=record.rho_map.map_file.full_path
        #    dmp_map_file=record.dmp_map.map_file.full_path
        #    production_data={
        #        'rho_map_file':rho_map_file,
        #        'dmp_map_file':dmp_map_file,
        #    }
        #elif NAME_DISPOL == UNIQUE_NAME:
        #    production_data=dict(
        #        wfn_entry=record.wave_function,
        #        fchk_file_id=record.wave_function.wave_function_file.id,
        #        part=record.partitioning,
        #        part_weights=record.partitioning.isa_weights,
        #    )
        else:
            raise Exception(f"Do not know how to process property \'{type(data.record).__name__}\'")
        return data
    except Exception as ex:
        raise Exception(f"Error in getting necessary related data for production of {type(data.record).__name__}: {analyse_exception(ex)}")


@validate_call
def get_next_record(session, object, prop_args:my_dict={}):
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


@val_call
def create_new_worker(session, request, property, method=None, for_production=True ) -> return_data|None:
        
    # Get the record and worker id for the next record
    try:
        UNIQUE_NAME=get_unique_tag(property)
        the_object=get_object_for_tag(UNIQUE_NAME)
        if UNIQUE_NAME==NAME_WFN:
            #object=Wave_Function
            prop_args={}
        elif UNIQUE_NAME==NAME_PART:
            # Get result
            #object=Hirshfeld_Partitioning
            prop_args={'method':method}
        elif    NAME_IDSURF == UNIQUE_NAME:
            #object=IsoDens_Surface
            prop_args={}
        elif    NAME_ESPRHO == UNIQUE_NAME:
            #object=RHO_ESP_Map
            prop_args={}
        elif    NAME_ESPDMP == UNIQUE_NAME:
            #object=DMP_ESP_Map
            prop_args={}
        elif    NAME_ESPCMP == UNIQUE_NAME:
            #object=DMP_vs_RHO_ESP_Map
            prop_args={}
        elif NAME_DISPOL == UNIQUE_NAME:
            prop_args={}
        elif NAME_MOLPOL == UNIQUE_NAME:
            prop_args={}
        else:
            raise Exception(f"Cannot process property \'{property}\'")

        # Get the next record and if there is another record create a worker
        record= get_next_record(session, the_object, prop_args=prop_args)
        if record is None:
            worker_id=None
            return None
        else:
            host_address=f"{request.client.host}:{request.client.port}"
            worker_id=create_worker(session, host_address, record)

            data=return_data(
                worker_id=str(worker_id),
                record=record,
                record_type=type(record).__name__,
            )
            if for_production:
                data=make_production_data(data)
            return data


    except Exception as ex: my_exception(f"Error in retrieving record and worker id:", ex)
    

    # If production tag has been required enrich the folder

