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

            origin=os.getcwd()
            errors=[]

            import traceback, types
            def _get_root_exception(exc):
                # Follow explicit chaining ("raise X from Y")
                cur = exc
                while cur.__cause__ is not None:
                    cur = cur.__cause__

                # Follow implicit chaining ("During handling of this exception...")
                while cur.__context__ is not None:
                    cur = cur.__context__

                return cur

            def _strip_pydantic_frames(exc):
                root = _get_root_exception(exc)
                tb = root.__traceback__

                filtered_tb = None
                last = None

                while tb is not None:
                    filename = tb.tb_frame.f_code.co_filename
                    
                    if "_validate_call.py" not in filename:
                        # rebuild traceback node
                        new_tb = types.TracebackType(last, tb.tb_frame, tb.tb_lasti, tb.tb_lineno)
                        last = new_tb
                        filtered_tb = filtered_tb or new_tb

                    tb = tb.tb_next

                # Attach cleaned traceback to the original exception
                exc.__traceback__ = filtered_tb
                return exc
            
            try: # Make the directory where things will be execute in
                os.makedirs(tracker.working_dir, exist_ok=False)
                os.chdir(tracker.working_dir)
            except Exception as ex: 
                raise Exception(f"Problem in setting up environment {ex}") from ex

            try: # Execute the job
                results, job_already_done= run_job(tracker, data, max_iter, delay, do_test)
            except Exception as ex: 
                raise Exception(f"Problem in running job: {ex}") from ex
                root=ex
                chain=[]
                while root.__cause__ is not None:
                    root=root.__cause__
                    chain+=[root]
                raise chain[-2]
                print(f"Caused by: {type(chain[-2])} {chain[-2]} ({dir(chain[-2])})")
                chain[-2].add_note('test'*100)
                print(chain[-2].__traceback__)
                print(chain[-2].__context__)
                print(chain[-2].__suppress_context__)
                print(f"Caused by: {chain[-2].__notes__}")
                sys.exit(1)
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

     
        
