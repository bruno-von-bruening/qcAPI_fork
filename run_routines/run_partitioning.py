
import sys, os ; sys.path.insert(1, os.environ['SCR'])
import modules.mod_utils as m_utl
import modules.mod_objects as m_obj
from os.path import isfile, isdir
import subprocess, json, shutil, glob
import numpy as np

from utility import make_jobname, make_dir

def exc_partitioning(record, worker_id, num_threads=1, maxiter=150, target_dir=None, do_test=False):

    def make_horton_input_file(rep_di, method, fchk_file=None):
        # Make MBIS input file
        moment_file=rep_di['dir_nam']+'.mom'
        solution_file=rep_di['dir_nam']+'_solution.json'
        kld_history_file=rep_di['dir_nam']+'_kld-his.json'
        if isinstance(fchk_file, type(None)):
            fchk_file=rep_di['fchk_fi']
        inp_di={
            'METHOD':method,
            'WAVEFUNCTION':fchk_file,
            'ANG_GRID_DENS':rep_di['GRIDHORTON'],
            'MOMENTS_NAME': moment_file,
            'SOLUTION_NAME': solution_file,
            'KLD_HISTORY_NAME': kld_history_file,
        }
        input_file= f"INPUT_{rep_di['dir_nam']}.inp"
        with open(input_file, 'w') as wr:
            for k in inp_di.keys():
                wr.write(f"{k:<20} {inp_di[k]:}\n")
        return input_file, moment_file, solution_file, kld_history_file
    try:
        try:
            # Check conda
            env_name='horton_wpart'
            python=m_utl.get_conda_env(env_name)

            # Check script
            script=f"{os.environ['SCR']}/execution/run_horton_wpart.py"
            assert isfile(script), f"Not a file: {script}"

            # Make jobname
            id=record['id']
            jobname=make_jobname(id, worker_id)

            # Change to working directory
            work_dir=make_dir(jobname)
            os.chdir(work_dir)

            # Get fchk_file and link it
            file=record['fchk_file']; assert isfile(file), f"Not a file {file}"
            linked_file=f"ln_{os.path.basename(file)}"
            if os.path.islink(linked_file):
                os.unlink(linked_file)
            p=subprocess.Popen(f'ln -s {file} {linked_file}', shell=True)
            p.communicate()
            assert p.returncode==0
            rep_di=dict(
                GRIDHORTON='insane',
                fchk_fi=linked_file,
                dir_nam=jobname,
            )

            # Make horton
            method=record['method']
            input_file, moment_file, solution_file, kld_history_file= make_horton_input_file(rep_di, method, fchk_file=None)
        except Exception as ex:
            raise Exception(f"Error in Preparing data: {ex}")
            
        try:
            # Execute Horton
            cmd=f"{python} {script} -inp {input_file}"
            out_file='horton.out'
            horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            stdout, stderr = horton.communicate()
            if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")
        except Exception as ex:
            raise Exception(f"Error in executing horton: {ex}")



        try:
            error=[]

            # Retrieve files
            assert os.path.isfile(results_file)
            with open(results_file, 'r') as rd:
                results=json.load(rd)
            # Files
            try:
                mom_file=results['files']['moments']
                mom_file=os.path.join( os.path.dirname(results_file),mom_file )
            except Exception as ex:
                raise Exception(f"Cannot read moments in file {results_file}: {ex}")
            assert isfile(mom_file), f"{mom_file}"
            
            try:
                moms=m_obj.multipoles(mom_file).get_moments
                moms=np.array(moms).tolist()
                moms_json=json.dumps(moms)
            except Exception as ex:
                raise Exception(f"Error in reading moment from file {mom_file} with {m_obj.multipoles}: {ex}")
            
            try:
                os.chdir('..')
                if os.path.realpath(target_dir)!=os.getcwd():
                    target=make_dir(jobname, base_dir=target_dir)
                    assert os.path.isdir(target)
                    source_files= glob.glob(f"{work_dir}/*")
                    assert len(source_files)>0, f"No output files in {os.path.realpath(work_dir)}!"
                    [ shutil.move(file, target) for file in source_files ]
                    os.rmdir(work_dir)
            except Exception as ex:
                error.append(f"Problems in copying results: {str(ex)}")


            multipoles=dict(
                partitioning_id=record['id'],
                length_units='ANGSTROM',
                representation='CARTESIAN',
                convention='Stone',
                traceless=True,
                multipoles=moms_json,
            )
            
            # Process message and error
            if len(error)>0:
                message=f"Successful run (albeit not critical errors occured)"
                error='|'.join(error)
            else:
                message=f"Succesful run"
                error=None
            # Update convergence of record 
            record['converged']=1    
            return {
                'part':record,
                'multipoles':multipoles,
                'message': message,
                'error': error,
            }
        except Exception as ex:
            raise Exception(f"Error in postprocessing horton run : {str(ex)}")
    except Exception as ex:
        record['converged']=0
        return {
            'part': record,
            'multipoles': None,
            'message': f"Record did not converge",
            'error': str(ex),
        }
