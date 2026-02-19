from . import *

from run_routines.wave_function.run_wfn import compute_wave_function
from run_routines.part.run_partitioning import exc_partitioning
from run_routines.part.run_partitioning_camcasp import exc_partitioning_camcasp
from run_routines.surfaces.run_isodens_surf import run_isodens_surf
from run_routines.surfaces.run_esp_surf import run_esp_surf
from run_routines.surfaces.run_dmp_esp_surf import run_dmp_esp
from run_routines.surfaces.run_espcmp import run_espcmp
from run_routines.dispol.run_dispol import run_dispol
# sys.path.insert(1, os.environ['SCR'])
# import modules.mod_objects as m_obj
# import modules.mod_utils as m_utl

from util.util import print_flush


from .util import *
from .prepare_input import *


@val_call
def wait_for_job_completion(tracker:Tracker, record:SQLModel, res, delay) ->Tuple[Tracker,Union[dict|job_results|None], bool]:
    """ Check status of job until it changes.
    The job may be done by another worker, then return this info in job_already_done variable"""


    def time_diff_to_readable(t):
        if t<60:
            return f"{t:.1f} seconds"
        elif t<3600:
            return f"{t/60:.1f} minutes"
        else:
            return f"{t/3600:.1f} hours"


    def am_I_on_terminal():
        return sys.stderr.isatty()
    
    def make_output_print():
        if am_I_on_terminal():
            return lambda msg: print_flush(msg, end='\r')
        else:
            return lambda msg: print(msg)
    def make_message(job_status, time_passed=None):
        return f"JOB STATUS: {job_status} (record_type={type(record).__name__} record_id={get_primary_key(record)})" +(
            f" | Time passed: {time_diff_to_readable(time_passed)}" if not time_passed is None else ""
        )


    class my_clock:
        def __init__(self):
            self.start=time.time()
            self.last_checked=self.start
        def time_passed(self):
            return time.time()-self.start
        def clock_last_checked(self):
            self.last_checked=time.time()
    
    def check_job_already_done(clock:my_clock, force_print=False):
        response = requests.get(f"{tracker.server_address}/get_status/{type(record).__name__}/{get_primary_key(record)}?worker_id={tracker.worker_id}")
        if response.status_code != HTTPStatus.OK:
            print_flush(
                f"Error getting record status. Got status code: {response.status_code} , text={response.text}"
            )
            job_already_done=False
        else:
            job_status = response.json()
            if force_print:
                do_print=True
            elif on_terminal:
                do_print=True
            elif (time.time()-clock.last_checked-60)>0:
                clock.clock_last_checked()
                do_print=True
            else: do_print=False

            if do_print:
                msg=make_message(job_status, time_passed=time.time()-clock.start)
                print_message(msg)

            job_already_done = ( job_status in [RecordStatus.succeeded,RecordStatus.failed] )
        return job_already_done
        

    @val_call
    def internal_loop(tracker) -> Tuple[Tracker,Union[dict|job_results|None], bool]:
        """ Check if the job finished continously. For certain increments check if job has been done by other worker"""
        delay_rand = np.random.uniform(0.8, 1.2) * delay
        t0=time.time()
        clock=my_clock()
        force_print=True
        while True:
            try:
                tracker,ret = res.get(timeout=0.1) # in sec
                return tracker,ret, False
            except mp.TimeoutError:
                if time.time()-t0>0:
                    job_already_done=check_job_already_done(clock, force_print=force_print)
                    force_print=False
                    t0=time.time()+delay_rand
                    if job_already_done:
                        tracker.add_message(f"Job was already done by other worker")
                        return tracker,None,True 
            except Exception as ex:
                raise Exception(f"Error in getting results from thread: {ex}") from ex
    def print_info(record:job_results):
        # Recovering the avaible information
        info_lines=[]
        for key in  ['message','error','warnings']:
            if hasattr(record,key):
                info_lines.append(f"{key.capitalize():<10} : {getattr(record,key)}")
        if len(info_lines)==0:
            info_lines=[f"No information tags where found in the output"]
        
        # Formatting and printing
        info_string=f"Job completition:"
        indent=4*' '
        info='\n'.join([info_string]+ [ indent+x for x in info_lines])
        print(info)
            
    on_terminal=am_I_on_terminal()
    print_message=make_output_print()
    tracker,ret, job_done_by_other_worker=internal_loop(tracker)

    if not job_done_by_other_worker:
        print_info(ret.record)

    return tracker, ret, job_done_by_other_worker

@val_call
def get_next_record(
    serv_adr, property='part', method='lisa', for_production:bool=True
) -> return_data|None:
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
            return return_data(**body)
        elif status_code == HTTPStatus.NO_CONTENT:
            print_flush("No more records. Exiting.")
            return None
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





@val_call
def run_job(
        tracker:Tracker, data:return_data, max_iter, delay, do_test
)->Tuple[Tracker,Union[job_results|None], bool]:
    """ 
    Assumes that you already navigated to the appropiate working directory
    """
    
    # Based on the job data provided decide which script to run (returns callable)
    script=prepare_script(tracker, data, max_iter)

    # Start the job
    pool = mp.Pool(1) # Why is this here
    try:
        assert hasattr(script, '__call__'), f"Provide function for execution, got {script}"
        proc = pool.apply_async(script, error_callback=lambda e:None)
        
        # Check return of job
        tracker,results, job_already_done =wait_for_job_completion(tracker,data.record ,proc, delay)
    except Exception as ex: 
        # if do_test:
        #     try:
        #         script()
        #         raise Exception(f"Error : {ex}")
        #     except Exception as ex2:
        #         raise Exception(f"Error when running {script}: {ex2}")
        raise Exception(ex) from ex

    finally:
        pool.terminate()
        pool.join()

    return tracker,results, job_already_done
