
from util.import_helper import *
from . import *
from util.imports.http_imports import *
from util.imports.mysql_imports import *
from data_base.qcAPI_database import RecordStatus

from server_processes.util.util import get_object_for_tag

from receiver.get_request import upload_file


# Work on that
def process_job_results(entry:dict, serv_adr, worker_id:str, do_test=False):
    """ Process the results of the job and upload them to the server """

    def get_name_from_entry(entry:dict) -> str:
        assert '__name__' in entry, f"Expected entry to have a key '__name__', got {list(entry.keys())}"
        UNIQUE_NAME=entry['__name__']
        assert isinstance(UNIQUE_NAME, str), f"Expected entry['__name__'] to be a string, got {type(UNIQUE_NAME)}"
        return UNIQUE_NAME
    def get_primary_key(the_model:sqlmodel_cl_meta, entry:dict) -> str|int:
        prim_key=get_primary_key_name(the_model)
        assert prim_key in entry, f"Expected entry to have a key '{prim_key}', got {list(entry.keys())}"
        id=entry[prim_key]
        return prim_key, id
    
    def check_record_convereged(entry:dict) -> bool:
        if entry['converged']!=1:
            message=f"Failed to compute property {UNIQUE_NAME}:"
            for e in json.loads(entry['errors']):
                message+=f'\n{e}'
            raise Exception(message)

    class return_data(BaseModel): 
        run_directory: str
        files_for_storage: List[ str ] = []
        files_for_entries: dict[str, str] = {}
        sub_entries: dict[str, dict] = {}

    def prep_return(entry:dict):

        kwargs={}

        # Currently we do not do anything with run_info
        if 'run_info' in entry.keys():
            del entry['run_info']
        
        # !! FIX this mess
        if 'run_data' in entry.keys():
            run_data=entry['run_data']
            del entry['run_data']
        else: run_data={}

        if 'files' in run_data:
            kwargs.update({'files_for_entries':run_data['files']})
        
        # Run directory
        run_dir_key='run_directory'
        assert run_dir_key in run_data.keys(), f"Expected key '{run_dir_key}' in run_data, got {list(run_data.keys())}"
        run_directory=run_data[run_dir_key]
        assert os.path.isdir(run_directory), f"Expected run directory {run_directory} to be a directory, but it is not"
        kwargs.update({run_dir_key:run_directory})

        if 'to_store' in run_data.keys():
            to_store=run_data['to_store']
            if  isinstance(to_store, list):
                files_for_upload=[file for file in to_store if os.path.isfile(file)]
            elif isinstance(to_store, str):
                assert os.path.isdir(to_store), f"expected directory, got: {to_store}"
                files_for_upload=[ os.path.realpath(x) for x in glob.glob(f"{to_store}/*")]
            else: raise Exception()
            kwargs.update({'files_for_storage':files_for_upload})

        sub_entries_key='sub_entries'
        if sub_entries_key in run_data.keys():
            
            sub_entries=run_data[sub_entries_key]

            assert isinstance(sub_entries, dict) or sub_entries is None, sub_entries
            if isinstance(sub_entries, dict):
                for k,v in sub_entries.items():
                    assert isinstance(v,dict) or v is None, f"Expected dict or none, got: {v}"
                    if isinstance(v, dict):
                        the_model=get_object_for_tag(k)
                        prim_key=get_primary_key_name(the_model)
                        if not prim_key in v.keys():
                            v.update( {prim_key:id})
                            raise Exception(run_directory)
            kwargs.update({'sub_entries':sub_entries})

        return entry,return_data(**kwargs)



    UNIQUE_NAME=get_name_from_entry(entry)
    the_model=get_object_for_tag(UNIQUE_NAME)
    prim_key, id=get_primary_key(the_model,entry)

    entry, return_object=prep_return(entry)
    if do_test:
        try:
            check_record_convereged(entry)
        except Exception as ex: raise Exception(f"Record did not converged. Terminating since test was requested."+
                                                f"\nThe run directory is {os.path.realpath(return_object.run_directory)}:\n {ex}")
    




    if   NAME_WFN       == UNIQUE_NAME:
        request=f"{serv_adr}/fill/wfn/{worker_id}"
    elif NAME_PART      == UNIQUE_NAME:
        request=f"{serv_adr}/fill/part/{worker_id}"
    elif UNIQUE_NAME==NAME_IDSURF:
        request=f"{serv_adr}/fill/isosurf/{worker_id}"
    elif NAME_ESPRHO == UNIQUE_NAME:
        request=f"{serv_adr}/fill/esprho/{worker_id}"
    elif NAME_ESPDMP == UNIQUE_NAME:
        request=f"{serv_adr}/fill/espdmp/{worker_id}"
    elif NAME_ESPCMP == UNIQUE_NAME:
        request=f"{serv_adr}/fill/espcmp/{worker_id}"
    elif NAME_DISPOL == UNIQUE_NAME:
        request=f"{serv_adr}/fill/dispol/{worker_id}"
    else:
        raise Exception(f"Did not implement case for {UNIQUE_NAME}")
    

    # EXTRACT all the data necessary so that no local files are necessary anymore
    # Store all files that should be stored by uploading them to the central database
    # After that, delete the files to free up space
    try: # Try and if fails then clean up

        # Upload the data for storage
        from .client_server_ext import pack_run_directory
        the_file=pack_run_directory(return_object.run_directory, return_object.files_for_storage, the_model, id, worker_id)
        upload_file( serv_adr, f"{the_model.__name__}_Run_Data", id,the_file, delete_old=True)

        # Push files will allways be the same array and files will not be changed
        #the_push=partial(push_file_from_tag, files)
        if entry['converged']==RecordStatus.converged:
            for tag, file in return_object.files_for_entries.items():
                upload_file(serv_adr,tag, id,file)
        # Upload the lead entry
        data=dict(
            main_entry= entry,
            sub_entries= return_object.sub_entries,
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
            raise Exception(f"Error updating record ({request}) with code {status_code}: {error}\n{response.text}")

    except Exception as ex:
        if not do_test:
            try:
                run_shell_command(f"rm -r {return_object.run_directory}")
            except Exception as ex:
                raise Exception(f"Problem in deleting job run directory ({os.path.realpath(return_object.run_directory)}). This is critical since if these directories are not cleaned this will polute the fiel system")
            raise Exception(f"Error in processing results: {str(ex)}")
        else:
            # In case this is a test, then keep the run_directory to be able to perform error tracing:
            #   1. add the file name to a file that could be used for clean-up
            #   2. raise the exception and inform the user where run directory is and that it has to be deleted manually
            
            def store_path_to_be_deleted():
                qcAPI_hidden_dir=f"{os.environ['HOME']}/.qcpAPI"
                if not os.path.isdir(qcAPI_hidden_dir):
                    os.mkdir(qcAPI_hidden_dir)
                to_delete_file=os.path.join( qcAPI_hidden_dir, f"files_to_be_deleted.txt")
                if not os.path.isfile(to_delete_file): run_shell_command(f"touch {to_delete_file}")
                # Append directory to be deleted to the file keeping track of this 
                run_shell_command( f"sed -i '$ a\{return_object.run_directory}' {to_delete_file}" ) # avoided appending with >> since my function does not like it
            store_path_to_be_deleted()
            raise Exception(f"Failure in processing return of job and test requested. Error was: {ex}\n The job run directory {os.path.realpath(return_object.run_directory)} was not deleted and could serve for error tracing. Please delete after you finished you anlaysis.")
