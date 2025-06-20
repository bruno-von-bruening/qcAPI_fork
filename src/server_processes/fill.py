from . import *
import uuid

from .fill_ext import fill_espdmp, fill_esprho, fill_idsurf, fill_part, fill_espcmp


def kill_woker(session,worker_id):
    # Kill worker
    worker = session.get(Worker, uuid.UUID(worker_id))
    if worker is None:
        raise HTTPException(HTTPStatus.PRECONDITION_FAILED, detail="Worker does not exist")
    session.delete(worker)
    session.commit()

@val_call
def wrapper_gen_fill(entry, session, worker_id, property, tracker, sub_entries=None|dict):
    @val_call
    def fill(tracker,the_object:SQLModelMetaclass, entry:dict, lead=True):
        """
        lead means that is a core object and not a dependant (e.g. outsourced file or data table )
        """
        try:
            prim_key= get_primary_key_name(the_object)
            id=entry[prim_key]
            prev_record = session.get(the_object, id)
            if lead:
                if prev_record is None:
                    raise HTTPException(status_code=HTTPStatus.CONFLICT, detail="Record does not exist")
                if prev_record.converged == 1:
                    raise HTTPException(status_code=HTTPStatus.NO_CONTENT, detail="Record already converged")
        except HTTPException as ex: raise ex
        except Exception as ex: my_exception(f"Problem in finding previous record",ex)

        try:
            record=the_object(**entry)
            converged_key='converged'
            if hasattr(record, converged_key):
                record_status=getattr(record, 'converged')
                assert record_status in [RecordStatus.converged,RecordStatus.failed], f"Unexpected status, {record_status}"
        except Exception as ex: my_exception(f"Problem in data of record {the_object}", ex)
            #record.warnings=json.dumps( json.loads(record.warnings)+warnings )
            
        try:
            if prev_record is not None:
                update_record(session, prev_record, record)
            else:
                create_record(session, the_object, entry)
        except Exception as ex: my_exception(f"Problem in updating record",ex)

        return tracker
    @val_call
    def fill_sub_entries(tracker,sub_entries:dict):
        try:
            for k,v in sub_entries.items():
                the_model=get_object_for_tag(k)
                tracker=fill(tracker, the_model, v, lead=False)
        except Exception as ex: my_exception(f"Problem in filling sub_entries:",ex)

    try:
        UNIQUE_NAME=get_unique_tag(property)
        kill_woker(session=session, worker_id=worker_id)
        warnings=[]
        if UNIQUE_NAME==NAME_PART:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged:
                tracker=fill_part(session, tracker, the_model, entry, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif NAME_DISPOL == UNIQUE_NAME:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif NAME_WFN == UNIQUE_NAME:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif UNIQUE_NAME==NAME_IDSURF:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif NAME_ESPRHO==UNIQUE_NAME:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif NAME_ESPDMP==UNIQUE_NAME:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        elif NAME_ESPCMP == UNIQUE_NAME:
            the_model=get_object_for_tag(UNIQUE_NAME)
            if entry['converged']==RecordStatus.converged and sub_entries is not None:
                fill_sub_entries(tracker, sub_entries)
            tracker=fill(tracker, the_model, entry)
        else:
            raise Exception(f"Unkown property \'{UNIQUE_NAME}\'")
        return tracker
    # Catch HTTP Exception to forward it. The receiver then has to print/interprete it
    except HTTPException as ex:
        raise HTTPException(ex.status_code, f"HH {ex.status_code}")
    except Exception as ex:
        raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not execute {wrapper_gen_fill}: {analyse_exception(ex)}")
@val_call
def upload_file_ext(storage_info,file, the_model, id):
    # where to drop (put info into extend_app of this function -> storage_info
    # drop_path=generate_drop_path(**storage_info)

    def make_drop_directory(storage_info, the_model) -> Tuple[str,str]:
        root_directory=storage_info.storage_root_directory

        is_run_data= ( the_model.__name__.lower().endswith('run_data') )
        container='run_data' if is_run_data else 'files'

        lower_path=(container,the_model.__name__)

        # Check if there or make directory
        total_path=root_directory
        for member in lower_path:
            assert os.path.isdir(total_path)
            new_path=os.path.join(total_path,member)
            if not os.path.isdir(new_path):
                os.mkdir(new_path)
            total_path=new_path
        
        return root_directory, os.path.join(*lower_path)
            
    def make_file_name(file, the_model, id):
        base=os.path.basename( file.filename )
        extensions=base.split('.')
        assert len(extensions)>1, f"No file extension in {base} (from {file.filename})"
        ext='.'.join(extensions[1:])

        file_name=f"{the_model.__name__}_id-{id}"+'.'+ext
        return file_name
    def drop_file(file: UploadFile, drop_path, file_name=None):
        contents = file.file.read()
        if file_name is None: file_name=os.path.basename(file.filename)
        new_file=os.path.join(drop_path, file_name)
        with open(new_file, 'wb') as wrb:
            wrb.write(contents)
        return new_file

    drop_root,drop_sub_path=make_drop_directory(storage_info, the_model)
    drop_path=os.path.join(drop_root, drop_sub_path)
    file_name=make_file_name(file, the_model, id)

    the_file=drop_file(file, drop_path, file_name=file_name)

    # Pack the results and return
    file_model=the_model(
        path_to_container=drop_root,
        path_in_container=drop_sub_path,
        file_name=os.path.basename(the_file)
    )
    setattr( file_model, get_primary_key_name(the_model), id)
    return file_model

from util.config import qcAPI_storage_info
def add_upload_functions(app, SessionDep,
    storage_info: qcAPI_storage_info # Information about storage    
):
    """ Http requests to be added to app """

    @app.put("/fill/{property}/{worker_id}")
    async def fill(
        data: dict, 
        worker_id: str, 
        property: str, 
        session: SessionDep, request: Request,
    ):
        try:

            @val_call
            def process_data(data:dict) -> Tuple[dict, dict]:
                entry_key='main_entry'
                sub_key='sub_entries'

                assert entry_key in data.keys(), f"Expected \'{entry_key}\'"
                entry=data[entry_key]
                assert isinstance(entry, dict), f"Expected dictionary as entry!"
                
                if not sub_key in data.keys():
                    sub_entries=None
                else:
                    sub_entries=data[sub_key]
                    # Check
                    if sub_entries is None:
                        pass
                    elif isinstance(sub_entries, dict):
                        assert all( isinstance(v, dict) or v is None for k,v in sub_entries.items() ), f"Expected dictionary of dictionaryies, got:\n{sub_entries}"
                    else: raise Exception(f"Unexpected datatype for sub entries ({type(sub_entries)}): {sub_entries}")

                return entry, sub_entries


            tracker=track_http_request()

            entry, sub_entries=process_data(data)
            tracker=wrapper_gen_fill(entry, session, worker_id, property, tracker, sub_entries=sub_entries)

            return tracker.dump()
        #except HTTPException as ex:
        #    raise HTTPException(HTTPStatis.INTERNAL_SERVER_ERROR,ex)
        except HTTPException as ex:
            raise HTTPException(ex.status_code, ex.detail)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed request: {str(ex)}")
    
    @app.post("/upload_file/{the_property}/{id}")
    async def upload_file(
        the_property: str, 
        id: str,
        session: SessionDep, request: Request,
        file: UploadFile = File(...)
    ):
        try:
            tracker=track_http_request()

            #UNIQUE_NAME=get_unique_tag(the_property)
            the_model=get_object_for_tag(the_property)
            assert issubclass( the_model, File_Model ), f"{the_model} is not a subclass of {File_Model}"

            try:
                new_record=upload_file_ext(storage_info, file, the_model, id)
            except Excpetion as ex: my_exception(f"Could not create the file", ex)
            finally: file.file.close()

            # Ideally this should include the deletion of the previous file, this would require a routine that could just be done by
            # - prev_record.delete
            try:
                #new_record.timestamp = datetime.datetime.now().timestamp()
                prev_record = session.get(the_model, id)
                if prev_record is not None:
                    update_record(session, prev_record, new_record)
                    # In case the old file had a different name delete it!
                    if os.path.realpath(prev_record.path)!=os.path.realpath(new_record.path):
                        prev_record.delete_file()
                else:
                    create_record(session, the_model, new_record)
            except Exception as ex: my_exception(f"Could not update the database", ex)

            return tracker.dump()
        except HTTPException as ex:
            raise HTTPException(ex.status_code, ex.detail)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Failed request: {str(ex)}")

    
    return app
