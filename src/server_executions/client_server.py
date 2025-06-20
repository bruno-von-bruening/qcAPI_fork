from . import *
from util.environment import get_python_from_conda_env
from receiver.get_request import upload_file

from qcp_global_utils.pydantic.pydantic import file as pdtc_file, directory as pdtc_directory
from .client_server_ext import (
    prepare_espdmp_script, prepare_espmap_script, prepare_idsurf_script, prepare_part_script, prepare_wfn_script, prepare_espcmp_script,
    prepare_dispol_script,
)
from util.util import print_flush

def main(config_file, url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
    """ """

    def wait_for_job_completion(res, delay, property):
        """ Check status of job until it changes.
        The job may be done by another worker, then return this info in job_already_done variable"""

        job_already_done = False # Should stay false for regular termination
        entry=None
        while not job_already_done:
            try:
                delay = np.random.uniform(0.8, 1.2) * delay
                entry = res.get(timeout=delay)
                break
            except mp.TimeoutError:
                response = requests.get(f"http://{url}:{port}/get_status/{property}/{record['id']}?worker_id={worker_id}")
                if response.status_code != HTTPStatus.OK:
                    print_flush(
                        f"Error getting record status. Got status code: {response.status_code} , text={response.text}"
                    )
                    continue
                job_status = response.json()
                print_flush("JOB STATUS: ", job_status)
                job_already_done = ( job_status in [1,0] )
        
        if isinstance(entry, dict):
            
            # Recovering the avaible information
            info_lines=[]
            for key in  ['message','error','warnings']:
                if key in entry.keys():
                    info_lines.append(f"{key.capitalize():<10} : {entry[key]}")
            if len(info_lines)==0:
                info_lines=[f"No information tags where found in the output"]
            
            # Formatting and printing
            info_string=f"Job completition:"
            indent=4*' '
            info='\n'.join([info_string]+ [ indent+x for x in info_lines])
            print(info)

        return entry, job_already_done

    def get_next_record(serv_adr, property='part', method='lisa', for_production=True):
        """ Get a the next record to be worked at (in case there is none, return none) """
        while True:

            request_code='/'.join([
                    'get_next', property ])
            opts=[('method',method), ('for_production',True)]
            opts=[ f"{k}={v}" for k,v in opts if v!=None]
            request_code+=f"?{'&'.join(opts)}"

            the_request=os.path.join(serv_adr, request_code)
            response = requests.get(the_request)
            status_code=response.status_code
            
            # Break because there are no jobs left
            if status_code == HTTPStatus.OK:
                body = response.json()
                record,worker_id = body
                break
            elif status_code == HTTPStatus.NO_CONTENT:
                print_flush("No more records. Exiting.")
                worker_id, record= (None, None)
                break
            elif status_code== HTTPStatus.INTERNAL_SERVER_ERROR:
                raise Exception(f"{HTTPStatus.INTERNAL_SERVER_ERROR} ({request_code}): (received code {status_code}, detail={response.text})")
            elif status_code== HTTPStatus.UNPROCESSABLE_ENTITY:
                raise Exception(f"Invalid request to server: {the_request}: detail={response.text}")
            elif status_code==HTTPStatus.NOT_FOUND:
                raise Exception(f"Server did not find request {request_code}: {status_code} {response.text}")
            else:
                error=f"Unkown Error"
                print(f"{error} ({request_code}): Retrying in a bit. (received code {status_code}, detail={response.text})")
            time.sleep(0.5)
        return record, worker_id
    serv_adr=f"http://{url}:{port}" # address of server
    mp.set_start_method("spawn")

    # Get a new record until there a no one left
    while True:
        # Check for a new record
        record,worker_id=get_next_record(serv_adr, method=method, property=property)
        # Break if nothing to be done anymore
        if isinstance(worker_id, type(None)):
            break

        UNIQUE_NAME =get_unique_tag(property)

        # Decide which function to use and define arguments
        prod_key='production_data' # Key under which production data is dumped
        assert prod_key in record.keys(), f"Could not find key \'{prod_key}\' in record keys ({list(record.keys())})"
        prod_data=record[prod_key]
        del record[prod_key]
        if UNIQUE_NAME==NAME_WFN:
            script = prepare_wfn_script(config_file, record, serv_adr, max_iter=max_iter)
        elif NAME_PART==UNIQUE_NAME:
            script = prepare_part_script(config_file, record, serv_adr, max_iter=max_iter)
        elif UNIQUE_NAME==NAME_IDSURF:
            fchk_file=prod_data['fchk_file']
            script = prepare_idsurf_script(config_file, fchk_file=fchk_file)
        elif NAME_ESPRHO==UNIQUE_NAME:
            fchk_file=prod_data['fchk_file']
            surface_file=prod_data['surface_file']
            script = prepare_espmap_script(config_file, fchk_file=fchk_file, surface_file=surface_file)
        elif NAME_ESPDMP==UNIQUE_NAME:
            moment_file=prod_data['moment_file']
            surface_file=prod_data['surface_file']
            script = prepare_espdmp_script(config_file, moment_file=moment_file, surface_file=surface_file)
        elif NAME_ESPCMP == UNIQUE_NAME:
            rho_map_file=prod_data['rho_map_file']
            dmp_map_file=prod_data['dmp_map_file']
            script = prepare_espcmp_script(config_file, dmp_map_file=dmp_map_file, rho_map_file=rho_map_file)
        elif NAME_DISPOL == UNIQUE_NAME:
            script=prepare_dispol_script(config_file, prod_data, serv_adr)
        else:
            raise Exception(f"No routine defined for {UNIQUE_NAME}")
        # After extraction clean the record

        args=(record, worker_id)
        kwargs={'do_test':do_test , 'num_threads':num_threads,'target_dir':target_dir}
        # Start the job
        pool = mp.Pool(1) # Why is this here
        assert hasattr(script, '__call__'), f"Provide function for execution, got {script}"
        proc = pool.apply_async(script, args=args, kwds=kwargs)
        # Check return of job
        entry, job_already_done =wait_for_job_completion(proc, delay, property)
        
        # In case the has been done by annother worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
        else:
            assert isinstance(entry, dict), f"Expect return of production script to return new row as dictionary, got {type(entry)}"

            if do_test:
                if entry['converged']!=1:
                    message=f"Failed to comput with script={script}:"
                    for e in json.loads(entry['errors']):
                        message+=f'\n{e}'
                    raise Exception(message)
            if not entry is None:
                if 'run_info' in entry.keys():
                    del entry['run_info']


            # !! FIX this mess
            if 'run_data' in entry.keys():
                run_data=entry['run_data']
            else: run_data={}
            if 'files' in run_data:
                files=entry['run_data']['files']
                #del entry['files']
            else:
                files={}
            file_uploads=[]



            the_model=get_object_for_tag(UNIQUE_NAME)
            id=record[get_primary_key_name(the_model)]
            #@val_call
            #def push_file(file_uploads:List[Tuple[str,dict]],  object:str|sqlmodel_cl_meta, the_file:file, id):
            #    try:
            #        upload_model=object if isinstance(object, str) else object.__name__
            #        file_uploads+=[
            #            (upload_model, dict(id=id,file=the_file))
            #        ]
            #    except Exception as ex: raise Exception(ex)
            #    return file_uploads
            #@val_call
            #def push_file_from_tag(files: dict,file_uploads:List[Tuple[str,dict]],  object:str|sqlmodel_cl_meta, tag:str, id):
            #    try:
            #        assert tag in files.keys(), f"Could not find \'{tag}\' in provided dictionary"
            #        the_file=files[tag]
            #    except Exception as ex: raise Exception(ex)
            #    return push_file(file_uploads, object, the_file,id)
            @val_call
            def pack_run_directory(working_directory:directory, run_directory: List[str]|directory) -> pdtc_file:
                
                # Check that the working direcotry is the 
                # This is not really elegant but a check is better than deleting something undesired
                if isinstance(run_directory, str):
                    assert os.path.realpath(run_directory)==os.path.realpath(working_directory)
                else:
                    for x in run_directory:
                        assert any([ y(os.path.join(working_directory,x)) for y in [os.path.isdir,os.path.isfile]]) , x
                    run_directory=[ os.path.join(working_directory,x) for x in run_directory]

                the_tar=f"{the_model.__name__}_{id}_WID-{worker_id}"
                if os.path.isdir(the_tar): run_shell_command(f"rm -r {the_tar}")
                if isinstance(run_directory,str):
                    run_shell_command(f"cp -r {run_directory} {the_tar}")
                    pack_files=run_directory
                else:
                    os.mkdir(the_tar)
                    run_shell_command( f"cp -r {' '.join(run_directory)} {the_tar}")

                run_shell_command(f"tar --create --file={the_tar}.tar {the_tar} --remove-files")
                run_shell_command(f"xz {the_tar}.tar")
                compressed_file=f"{the_tar}.tar.xz"
                return compressed_file


            
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

             

            
            sub_entries=None
            if 'run_data' in entry.keys():
                if 'to_store' in entry['run_data']:
                    assert 'run_directory' in entry['run_data'], f"Did not found key \'run_directory\' in return"
                    run_directory=entry['run_data']['run_directory']
                    the_file=pack_run_directory(run_directory, entry['run_data']['to_store'])
                    upload_file( serv_adr, f"{the_model.__name__}_Run_Data", id,the_file)
                if 'sub_entries' in entry['run_data']:
                    sub_entries=entry['run_data']['sub_entries']
                    assert isinstance(sub_entries, dict) or sub_entries is None, sub_entries
                    if isinstance(sub_entries, dict):
                        for k,v in sub_entries.items():
                            assert isinstance(v,dict) or v is None, f"Expected dict or none, got: {v}"
                            if isinstance(v, dict):
                                the_model=get_object_for_tag(k)
                                prim_key=get_primary_key_name(the_model)
                                if not prim_key in v.keys():
                                    v.update( {prim_key:id})
                del entry['run_data']
            if 'run_info' in entry.keys():
                run_info=entry['run_info']
                del entry['run_info']

            # Push files will allways be the same array and files will not be changed
            #the_push=partial(push_file_from_tag, files)
            if entry['converged']==RecordStatus.converged:
                for tag, file in files.items():
                    the_model=get_object_for_tag(tag)
                    assert isinstance(file, str), file
                    upload_file(serv_adr,the_model, id,file)
            
            data=dict(
                main_entry= entry,
                sub_entries= sub_entries,
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
