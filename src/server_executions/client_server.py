from . import *
from util.environment import get_python_from_conda_env
from receiver.get_request import upload_file

from qcp_global_utils.pydantic.pydantic import file as pdtc_file, directory as pdtc_directory
from .client_server_ext import (
    prepare_espdmp_script, prepare_espmap_script, prepare_idsurf_script, prepare_part_script, prepare_wfn_script, prepare_espcmp_script,
    prepare_dispol_script, get_next_record, job_wrapper, pack_run_directory,
)
from .client_server_core import (
    process_job_results,
)
from util.util import print_flush


def main(config_file, url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
    """ """
 
    # Server Address
    serv_adr=f"http://{url}:{port}"
    # Thomas said thats about concurrency problems
    mp.set_start_method("spawn")

    # Get a new record until there a no one left
    origin=os.getcwd()
    while True:
        os.chdir(origin)

        # Obtain the next record to work on
        record,worker_id=get_next_record(serv_adr, method=method, property=property)

        # Break if nothing to be done anymore
        if isinstance(worker_id, type(None)):
            break
        # Instructions for ne job
        UNIQUE_NAME =get_unique_tag(property)
        # Process next job
        pool, entry, job_already_done= job_wrapper(config_file, serv_adr, UNIQUE_NAME, max_iter, delay, record, worker_id, num_threads, target_dir, do_test)
        
        # In case the has been done by annother worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
            break
        else:
            assert isinstance(entry, dict), f"Expect return of production script to return new row as dictionary, got {type(entry)}"
            # check and upload
            entry.update({'__name__': UNIQUE_NAME})
            process_job_results(entry, serv_adr, worker_id, do_test=do_test)
     
        
