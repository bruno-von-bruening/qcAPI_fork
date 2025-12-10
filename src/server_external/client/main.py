from . import *

from util.config import qcAPI_server_config

from .body import (
    get_next_record, run_job
)
from .push_results import (
    process_job_results,
)


def main(
        # config_file:pdtc_file,
        config_file     :pdtc_file,
        num_threads :int,
        mem_GB      :float, # 
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

    config=qcAPI_server_config(config_file)
    address=config.address

    def main_core():
        """ """
        
        # Obtain the next record to work on
        data=get_next_record(address, method=method, property=property)

        if data is None: # That means no worker has been generated since there is nothing left to do
            return False
        else:
            tracker=Tracker(
                server_address=address,
                worker_id=data.worker_id,
                main_record_id=get_primary_key(data.record),
                target_dir=os.path.realpath(target_dir),
                num_threads=num_threads,
                memory_GB=mem_GB,
                test=do_test,
                config_file=os.path.realpath(config_file),
            )

            errors=[]

            origin=os.getcwd()
            try: # Make the directory where things will be execute in
                os.makedirs(tracker.working_dir, exist_ok=False)
                os.chdir(tracker.working_dir)
            except Exception as ex: 
                raise Exception(f"Problem in setting up environment {ex}") from ex

            try: # Execute the job
                results, job_already_done= run_job(tracker, data, max_iter, delay, do_test)
            except Exception as ex: 
                raise Exception(f"Problem in running job: {ex}") from ex
                
            if job_already_done: # In case the has been done by annother worker I still want to kill the worker 
                print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            else:
                process_job_results(tracker, results, tracker.server_address, tracker.worker_id, do_test=do_test)

            return True
 

    # Server Address
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

     
        
