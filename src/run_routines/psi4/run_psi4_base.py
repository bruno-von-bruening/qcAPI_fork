# NEW
from . import *

from .run_psi4_help import *

from util.trackers import message_tracker

from qcp_objects.objects.properties import geometry

from .psi4_helper import job_opts

@val_call
def run_psi4_base(
        python, psi4_script, 
        tracker: Tracker,
        wfn_record:Wave_Function, 
        geom:geometry,
        job_tag: job_opts,
        max_iter=150, 
        extra_cmdln_opts: dict={},
) -> Tuple[Tracker, Wave_Function, my_run_data]:
    """ Run a basic psi4 calculation according to the specified job type
    """
    # create ID

    try:
        the_sep=partial(my_sep, jobname=tracker.job_name, sep='#')
        try:
            wfn_record,tracker = compute_core(
                python, psi4_script,
                wfn_record, geom, job_tag, tracker, extra_cmdln_opts,
            )
        except Exception as ex: my_exception(f"Could not compute wave function",  ex)

        # Storage
        try: # Postprocessing
            storage_file=recover_storage(tracker.job_name)

            # this depends on the run type

            files=dict(
                storage_file=storage_file,
            )
        except Exception as ex: my_exception(f"Problem in recovering results",ex)

        converged=1
        message='SUCCESS in psi4 calculation'
    except Exception as ex:
        message='FAILED psi4 calculation'
        converged=0
        files={}
        tracker.add_error(str(ex))
        sub_entries=None
    finally:
        the_sep(message)
        wfn_record.status = converged
        # for k,v in tracker.model_dump(include={'messages', 'errors', 'warnings'}).items():
        #     if not hasattr(wfn_record, k):
        #         warn(f"Could not write key {k} in {type(wfn_record)}")
        #     else:
        #         setattr(wfn_record, k, v)
    
    try:
        run_data=my_run_data(
            run_directory=tracker.working_dir,
            files=files,
            #run_files_to_store={'working_dir':tracker.working_dir}
        )

        return tracker, wfn_record, run_data,  

    except Exception as ex:
        raise Exception(f"Could not pack results from calculation:\n{ex}")
