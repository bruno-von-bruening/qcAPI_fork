
from . import *

from orm_import.qcAPI_database import RecordStatus


from receiver.get_request import upload_file
from util.sql_util import get_primary_key




def get_name_from_record(entry:dict) -> str:
    assert '__name__' in entry, f"Expected entry to have a key '__name__', got {list(entry.keys())}"
    UNIQUE_NAME=entry['__name__']
    assert isinstance(UNIQUE_NAME, str), f"Expected entry['__name__'] to be a string, got {type(UNIQUE_NAME)}"
    return UNIQUE_NAME


@val_call
def check_record_convereged(tracker,entry:SQLModel) -> bool:
    if entry.status!=RecordStatus.succeeded:
        message=f"Failed to compute property {type(entry).__name__}:"
        errors=tracker.errors
        if isinstance(errors, str):
            errors=json.loads(errors)
        if len(errors)==0:
            message+=f"\nNo errors message pushed from calculation"
        else:
            for e in errors:
                message+=f'\n{e}'
        raise Exception(message)
    else:
        return True





# Work on that
def process_job_results(tracker:Tracker,results:job_results, serv_adr, worker_id:str, do_test=False, delete_local=True):
    """ 
    1. Process the results of the job to universal form for upload
    2. push them to the server 

    delete_local: if True, then delete files after upload
    """
    record=results.record
    the_model=type(record)

    import socket
    socket.gethostname()
    run_data_model=get_object_for_tag(f"{the_model.__name__}_Run_Summary")
    track_data=tracker.model_dump()
    walltime_worker=track_data.pop('elapsed_time')
    track_data['walltime_worker']=walltime_worker
    
    run_data=run_data_model(
        **track_data,
        id=get_primary_key(record),
        num_cores=tracker.num_threads,
        memory=tracker.memory_GB,
    )

    results.sub_entries.update({
        run_data_model.__name__: run_data
    })

    # The entry should correspond to a SQLModel class
    # the_model=get_object_for_tag(UNIQUE_NAME)

    results.inherit_id_to_subentries()


    if do_test:
        try:
            check_record_convereged(tracker,record)
        except Exception as ex: raise Exception(f"Record did not converged. Terminating since test was requested."+
                                                f"\nThe run directory is {os.path.realpath(results.run_data.run_directory)}:\n {ex}") from ex

    request=f"{serv_adr}/fill/{the_model.__name__}/{worker_id}"
    # if   NAME_WFN       == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/wfn/{worker_id}"
    # elif NAME_PART      == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/part/{worker_id}"
    # elif UNIQUE_NAME==NAME_IDSURF:
    #     request=f"{serv_adr}/fill/isosurf/{worker_id}"
    # elif NAME_ESPRHO == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/esprho/{worker_id}"
    # elif NAME_ESPDMP == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/espdmp/{worker_id}"
    # elif NAME_ESPCMP == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/espcmp/{worker_id}"
    # elif NAME_DISPOL == UNIQUE_NAME:
    #     request=f"{serv_adr}/fill/dispol/{worker_id}"
    # else:
    #     raise Exception(f"Did not implement case for {UNIQUE_NAME}")
    

    # EXTRACT all the data necessary so that no local files are necessary anymore
    # Store all files that should be stored by uploading them to the central database
    # After that, delete the files to free up space
    os.chdir('..')
    id=get_primary_key(record)
    try:
        from .body import pack_run_directory
        out_fi=os.path.join( results.run_data.run_directory, 'results_for_database.json' )
        with open(out_fi, 'w') as wr:
            json.dump(results.model_dump(), wr, indent=4)
        results.run_data.run_files_to_store.update({f"dumped_results": out_fi}) 
        the_file=pack_run_directory(results.run_data.run_directory, results.run_data.run_directory, the_model, id, worker_id)
    except Exception as ex: 
        raise Exception(f"Could not pack run directory for upload: {ex}") from ex 
    try: # Try and if fails then clean up

        # Upload the data for storage
        model=get_object_for_tag(f"{the_model.__name__}_Run_Files")
        upload_file( serv_adr, model, id, the_file, delete_old=True)

        # Push files will allways be the same array and files will not be changed
        #the_push=partial(push_file_from_tag, files)
        if record.status==RecordStatus.succeeded:
            for tag, file in results.files_for_entries.items():
                if isinstance(file, SQLModel):
                    id=get_primary_key(file)
                    file=file.realpath
                upload_file(serv_adr,get_object_for_tag(tag) if isinstance(tag, str) else tag, id,file)
        # Upload the lead record
        data=dict(
            main_record= record.model_dump(),
            sub_entries= dict( (k, (v if not issubclass(type(v),BaseModel) else v.model_dump() ))
                          for k,v in results.sub_entries.items()
            ),
        )
        response = requests.put(request, json=data )
        # Check success of request
        status_code=response.status_code
        if status_code == HTTPStatus.OK: # desired
            print(f"Normal Return:\n  Message={response.json()['message']}\n  Error={response.json()['error']}")
            error=None
        elif status_code == HTTPStatus.NO_CONTENT:
            print(f"Record already converged:\n Will not update record and proceed to next task.")
            error=None
        elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            error=f"Error in processing"
        elif status_code == HTTPStatus.UNPROCESSABLE_ENTITY: # error in function definition
            error= f"Bad communication with function (check function argument)"
        else:
            error= f"Undescribed error"
        
        if not error is None:
            try:
                resp=json.loads(response.text)['detail']
            except:
                resp=response.text
            raise Exception(f"Error updating record ({request}) with code {status_code}: {error}\n{resp}")

    except Exception as ex:
        lead_text=f"Failure in processing return of job and uploading results to server. Error was: {ex}" 
        if do_test:
            # In case this is a test, then keep the run_directory to be able to perform error tracing:
            #   1. add the file name to a file that could be used for clean-up
            #   2. raise the exception and inform the user where run directory is and that it has to be deleted manually
            delete_local=False
            run_directory=results.run_data.run_directory
            def store_path_to_be_deleted():
                qcAPI_hidden_dir=f"{os.environ['HOME']}/.qcpAPI"
                if not os.path.isdir(qcAPI_hidden_dir):
                    os.mkdir(qcAPI_hidden_dir)
                to_delete_file=os.path.join( qcAPI_hidden_dir, f"files_to_be_deleted.txt")
                if not os.path.isfile(to_delete_file): run_shell_command(f"touch {to_delete_file}")
                # Append directory to be deleted to the file keeping track of this 
                run_shell_command( f"sed -i '$ a\{run_directory}' {to_delete_file}" ) # avoided appending with >> since my function does not like it
            store_path_to_be_deleted()
            raise Exception(f"{lead_text}\n The job run directory {os.path.realpath(results.run_data.run_directory)} was not deleted and could serve for error tracing. Please delete after you finished you anlaysis.") from ex
        else:
            raise Exception(lead_text) from ex
    finally:
        if delete_local:
            run_dir=results.run_data.run_directory
            try:
                if os.path.isdir(run_dir):
                    run_shell_command(f"rm -r {run_dir}")
            except Exception as ex:
                warn(f"Problem in deleting job run directory ({os.path.realpath(run_dir)}). This is not critical but if these directories are not cleaned this will polute the fiel system")
            print(f"Deleted job run directory: {os.path.realpath(run_dir)}")
