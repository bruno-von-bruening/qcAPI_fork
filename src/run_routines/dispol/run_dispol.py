from . import *
from .run_dispol_ext import make_isa_weight_file, get_fchk_file, run_camcasp

def working_directory(job_name, worker_id):
    
    # Make polarisability directory
    isapol_dir='pol'
    if not os.path.isdir(isapol_dir):
        os.mkdir(isapol_dir)
    
    # Make the working directory
    work_dir=f"{job_name}_WID-{worker_id}"
    work_dir=os.path.join(isapol_dir, work_dir)
    os.mkdir(work_dir)

    return work_dir

#@val_call
def run_dispol(
    python_exc, script_path, serv_url, wfn_entry, fchk_file_id, partitioning_entry, part_weights, record, worker_id, num_threads=1, max_iter=150, target_dir=None, do_test=False
):
    
    #if not target_dir is None: os.chdir(target_dir)
    #if not os.dir.

    # Get the fchk_file
    # write the weights to a file
    #raise Exception('test')
    tracker=Tracker()
    run_data={}
    try:

        try: # PREPARTION
            part_method = partitioning_entry['method']
            part_basis  = partitioning_entry['basis']

            jobname=f"ISA-POL_{part_method}_{part_basis}_{fchk_file_id}"
            origin=os.getcwd()
            work_dir=working_directory(jobname, worker_id)
            os.chdir(work_dir)

            weight_file=make_isa_weight_file(part_weights)
            tmp_fchk_file=get_fchk_file(serv_url, fchk_file_id)

            functional=wfn_entry['method']
            wfn_basis=wfn_entry['basis']
        except Exception as ex: tracker.add_error(f"Failed preparation: {analyse_exception(ex)}")
        
        #camcasp_exe=os.path.join(camcasp_path, 'bin','runcamcasp.py')
        #assert os.path.isfile(camcasp_exe), f"Not a valid file {camcasp_exe}"
        if tracker.no_error:
            try: 
                run_camcasp(f"{python_exc} {script_path}",tmp_fchk_file.tmp_file, weight_file)
                tmp_fchk_file.remove_tmp()
                os.chdir(origin)
            except Exception as ex: tracker.add_error(f"Failed run: {analyse_exception(ex)}")


        if tracker.no_error:
            try:
                # Postprocess
                mol_name='tmp'
                output_dir=os.path.join(work_dir, f"{mol_name}_isapol", 'OUT')
                assert os.path.isdir(output_dir), f"Expected output directory {output_dir}"
                the_file=f"{mol_name}_ISA-GRID_f11_NL4_fmtB.pol"
                the_file=os.path.join( output_dir, the_file )
                assert os.path.isfile(the_file),f"Not a file {the_file}"

                working_directory=os.getcwd()
                store_files=[ item for item in glob.glob('*') 
                             if not bool(re.search(r'.fchk',item.lower()))
                ]
                run_data.update(dict(
                    files={
                        Pairwise_Polarisabilities_File.__name__:os.path.realpath(the_file)
                    },
                    to_store=store_files,
                    run_directory=working_directory,

                ))
            except Exception as ex: tracker.add_error(f"Failed recovering results: {analyse_exception(ex)}")
    except Exception as ex:
        raise Exception(f"Unexpected Exception {ex}")

    converged=RecordStatus.converged if tracker.no_error else RecordStatus.failed
    record.update({'converged':converged,**tracker.model_dump()})
    
    run_info={'status':tracker.status, 'status_code':tracker.status_code}
    record.update({"run_data":run_data, 'run_info':run_info})

    return record