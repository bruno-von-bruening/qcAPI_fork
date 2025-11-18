from . import *

from .body import (
    get_next_record, run_job
)
from .push_results import (
    process_job_results,
)



def main(
        config_file:pdtc_file,
        url         :str, 
        port        :int, 
        num_threads :int,
        max_iter    :int, 
        delay       :float, 
        target_dir  :pdtc_directory|None=None, 
        do_test     :bool=False, 
        property:   str='wfn', 
        method:     str|None=None
):
    """ 
    This will request records to work on until the server does not have any left
    TODO: implement timeout time
    """


    def main_core():
        """ """
        
        # Obtain the next record to work on
        data=get_next_record(serv_adr, method=method, property=property)


        if data is None: # That means no worker has been generated since there is nothing left to do
            return False
        else:
            tracker=Tracker(
                server_address=serv_adr,
                worker_id=data.worker_id,
                main_record_id=get_primary_key(data.record),
                target_dir=os.path.realpath(target_dir),
                num_threads=num_threads,
                test=do_test,
                config_file=config_file,
            )

            origin=os.getcwd()
            try:
                try: # Make the directory where things will be execute in
                    os.makedirs(tracker.working_dir, exist_ok=False)
                    os.chdir(tracker.working_dir)
                except Exception as ex: raise Exception(f"Problem in setting up environment {ex}")

                try: # Execute the job
                    results, job_already_done= run_job(tracker, data, max_iter, delay, do_test)
                except Exception as ex: raise Exception(f"Could not exeucte the job: {ex}")
                
            except Exception as ex:
                raise ex
            finally: os.chdir(origin)

            if job_already_done: # In case the has been done by annother worker I still want to kill the worker 
                print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            else:
                process_job_results(results, tracker.server_address, tracker.worker_id, do_test=do_test)

            return True
 

    # Server Address
    serv_adr=f"http://{url}:{port}"
    mp.set_start_method("spawn") # Thomas said the spawn keyword is about avoiding concurrency problems

    # This loop will keep the worker occupied until the database does not have any pending entries
    origin=os.getcwd()
    while True:
        os.chdir(origin) # make sure to allways start where we spawned the client

        jobs_left=main_core()

        if jobs_left:
            continue
        else:
            break

     
        
