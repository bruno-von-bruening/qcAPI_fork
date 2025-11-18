from . import *
 
@val_call
def run_generic(
        tracker:Tracker,
        python:pdtc_file, 
        psi4_script:pdtc_file, 
        record:SQLModel, 
        geom:geometry,
        workder_id:str, 
        num_threads=1, max_iter=150, 
        do_test=False, 
) -> job_results:
    
    the_sep=partial(my_sep, jobname=tracker.job_name, sep='#')