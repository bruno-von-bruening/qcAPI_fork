from . import *
from util.environment import get_python_from_conda_env

from .client_server_ext import prepare_espdmp_script, prepare_espmap_script, prepare_idsurf_script, prepare_part_script, prepare_wfn_script, prepare_espcmp_script
from util.util import print_flush

def main(config_file, url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
    """ """

    def wait_for_job_completion(res, delay, property):
        """ Check status of job until it changes.
        The job may be done by another worker, then return this info in job_already_done variable"""

        job_already_done = False # Should stay false for regular termination
        while not job_already_done:
            try:
                delay = np.random.uniform(0.8, 1.2) * delay
                entry = res.get(timeout=delay)
                break
            except mp.TimeoutError:
                if property=='wfn':
                    response = requests.get(f"http://{url}:{port}/get_record_status/{record['id']}?worker_id={worker_id}")
                elif property=='part':
                    response = requests.get(f"http://{url}:{port}/get_part_status/{record['id']}?worker_id={worker_id}")
                else:
                    raise Exception()
                if response.status_code != HTTPStatus.OK:
                    print_flush(
                        f"Error getting record status. Got status code: {response.status_code} , text={response.text}"
                    )
                    continue
                job_status = response.json()
                print_flush("JOB STATUS: ", job_status)
                job_already_done = job_status == 1
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
        assert prod_key in record.keys()
        prod_data=record[prod_key]
        del record[prod_key]
        if UNIQUE_NAME==NAME_WFN:
            script = prepare_wfn_script(config_file, record, serv_adr, max_iter=max_iter)
        elif UNIQUE_NAME==NAME_PART:
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
        else:
            raise Exception(f"No routine defined for {UNIQUE_NAME}")
        # After extraction clean the record

        args=(record, worker_id)
        kwargs={'do_test':do_test , 'num_threads':num_threads,'target_dir':target_dir}
        # Start the job
        pool = mp.Pool(1) # Why is this here
        proc = pool.apply_async(script, args=args, kwds=kwargs)
        # Check return of job
        entry, job_already_done =wait_for_job_completion(proc, delay, property)

        if do_test:
            if entry['converged']!=1:
                message=f"Failed to comput with script={script}:"
                for e in json.loads(entry['errors']):
                    message+=f'\n{e}'
                raise Exception(message)
        if 'run_info' in entry.keys():
            del entry['run_info']
        
        # In case the has been done by another worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
        else:
            if UNIQUE_NAME==NAME_WFN:
                request=f"{serv_adr}/fill/wfn/{worker_id}"
            elif UNIQUE_NAME==NAME_PART:
                request=f"{serv_adr}/fill/part/{worker_id}"
            elif UNIQUE_NAME==NAME_IDSURF:
                request=f"{serv_adr}/fill/isosurf/{worker_id}"
            elif NAME_ESPRHO == UNIQUE_NAME:
                request=f"{serv_adr}/fill/esprho/{worker_id}"
            elif NAME_ESPDMP == UNIQUE_NAME:
                request=f"{serv_adr}/fill/espdmp/{worker_id}"
            elif NAME_ESPCMP == UNIQUE_NAME:
                request=f"{serv_adr}/fill/espcmp/{worker_id}"
            else:
                raise Exception(f"Did not implement case for {UNIQUE_NAME}")
        
        response = requests.put(request, json=entry )
        # Check success of request
        status_code=response.status_code
        if status_code == HTTPStatus.OK: # desired
            print(f"Normal Return:\n  Message={response.json()['message']}\n  Error={response.json()['error']}")
            error=None
        elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            error=f"Error in processing"
        elif status_code == HTTPStatus.UNPROCESSABLE_ENTITY: # error in function definition
            error= f"Bad communication with function (check function argument)"
        else:
            error= f"Undescribed error"
        if not isinstance(error, type(None)):
            raise Exception(f"Error updating record ({request}) with code {status_code}: {error}\n{response.text}")
        # Make sure to clean the file system!
        psi4.core.clean()