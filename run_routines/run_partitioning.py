import sys, os ; sys.path.insert(1, os.environ['SCR'])
origin=os.path.realpath('..'); sys.path.insert(1, origin)

import modules.mod_utils as m_utl
import modules.mod_objects as m_obj
from os.path import isfile, isdir
import subprocess, json, shutil, glob
import numpy as np

from utility import make_jobname, make_dir, load_global_config

def prepare_input(record, worker_id, config_file, warnings):
    def get_conda_python():
        """"""
        # Check conda
        env_name='horton_wpart'
        python=m_utl.get_conda_env(env_name)
        if isinstance(python, type(None)): raise Exception(f"Could not get python executable")
        assert os.path.isfile(os.path.realpath(python)), os.path.relpath(python)

        return python

    def get_script(config_file):
        # get horton script
        script=None
        if isinstance(config_file, type(None)):
            print(f"Did not receive valid config_file. This would be the best way to define horton script")
        else:
            global_config=load_global_config(config_file)
            horton_script_key='horton_script'
            if not horton_script_key in global_config.keys():
                print(f"Did not find key {horton_script_key} in {config_file}")
            else:
                script=global_config[horton_script_key]
        
        # Backup
        if isinstance(script, type(None)):
            warnings.append(f"Resorting to default value of horton script but should be designated in config file.")
            script=f"{os.environ['SCR']}/execution/run_horton_wpart.py"
        if isinstance(script, type(None)): raise Exception(f"Failed in finding horton script")
        assert isfile(script), f"Not a file: {script}"

        return script
    def prepare_horton_arguments():

        # Get fchk_file and link it
        file=record['fchk_file']; assert isfile(file), f"FCHK file in record is not valid: {file}"
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
        fchk_file=linked_file
        method=record['method']
        return jobname, method, rep_di, fchk_file
    def make_horton_input_file(rep_di, method, fchk_file):
        # Make MBIS input file
        assert os.path.isfile(fchk_file), f"FCHK file \'{fchk_file}\' does not exist"
        moment_file=rep_di['dir_nam']+'.mom'
        solution_file=rep_di['dir_nam']+'_solution.json'
        kld_history_file=rep_di['dir_nam']+'_kld-his.json'
        # if isinstance(fchk_file, type(None)):
        #     fchk_file=rep_di['fchk_fi']
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
        # Get environment dependent python and run script
        python = get_conda_python()
        script = get_script(config_file)
    except Exception as ex:
        raise Exception(f"Error while getting python executable and script: {ex}")

    try:
        # Make jobname
        id=record['id']
        jobname=make_jobname(id, worker_id)
        # Change to working directory
        work_dir=make_dir(jobname)
        os.chdir(work_dir)
        print(work_dir)
    except Exception as ex:
        raise Exception(f"Error while preparing directory: {ex}")


    try:
        # get the input needed to construct horton input
        jobname, method, rep_di,  fchk_file = prepare_horton_arguments()
        assert os.path.isfile(fchk_file)
    except Exception as ex:
        raise Exception(f"Error while preparing horton input arguments: {ex}")


    try:
        # construct horton input file
        input_file, moment_file, solution_file, kld_history_file= make_horton_input_file(rep_di, method, fchk_file)
    except Exception as ex:
        raise Exception(f"Error while generating horton input file: {ex}")
    
    return python, script, input_file, jobname, work_dir

####
def execute_horton(python, script, input_file):
    # Execute Horton
    output_file=input_file.split('.')[0]+'.hrtout'
    error_file=input_file.split('.')[0]+'.hrterr'
    
    cmd=f"export OMP_NUM_THREADS=1; {python} {script} -inp {input_file} 1> {output_file} 2> {error_file}"
    horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = horton.communicate()

    error_message=f"The commannd \'{cmd}\' did terminate with error: {stderr.decode('utf-8')}"
    if horton.returncode!=0: raise Exception(error_message)

def recover_horton_results(record, jobname, work_dir, target_dir, error, warnings):
    def get_moments():
        # Retrieve files
        results_file='results.json'
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
        return moms_json

    def copying_results(error):
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
        return error

    moms_json=get_moments()

    # copy the results to target directory and append errors if encountered
    error=copying_results(error)

    multipoles=dict(
        partitioning_id=record['id'],
        length_units='ANGSTROM',
        representation='CARTESIAN',
        convention='Stone',
        traceless=True,
        multipoles=moms_json,
    )
    return multipoles, error

def exc_partitioning(config_file, record, worker_id, num_threads=1, maxiter=150, target_dir=None, do_test=False):
    
    # Create warnings and errors that get change permanently in the exception blog
    warnings=[]
    errors=[]
    def array_to_str(array):
        return '|'.join(array)
    
    try:
        # Input generation
        try:
            python, script, input_file, jobname, work_dir = prepare_input(record, worker_id, config_file, warnings)
        except Exception as ex:
            raise Exception(f"Error in preparing data: {ex}")

        # Execution            
        try:
            execute_horton(python, script, input_file)
        except Exception as ex:
            raise Exception(f"Error in executing horton: {ex}")

        # Recovering Results
        try:
            error=[]
            multipoles, errors=recover_horton_results(record, jobname, work_dir, target_dir, errors, warnings)
        except Exception as ex:
            raise Exception(f"Error in postprocessing horton run : {str(ex)}")
        
        # Evaluate succes of run
        try:
            # Process message and error
            if len(error)>0:
                message=f"Successful run (albeit not critical errors occured)"
                error_string=array_to_str(errors)
            else:
                message=f"Succesful run"
                error_string=None
            # Update convergence of record 
            record['converged']=1    
            return {
                'part':record,
                'multipoles':multipoles,
                'message': message,
                'warnings': array_to_str(warnings),
                'error': error_string,
            }
        except Exception as ex:
            raise Exception(f"Error in returning results: {ex}")
    except Exception as ex:
        if do_test:
            raise Exception(f"Run failed with error: {ex}")
        else:
            print(f"Run failed with error: {str(ex)}")
        record['converged']=0
        return {
            'part': record,
            'multipoles': None,
            'message': f"Record did not converge",
            'error': str(ex),
            'warnings': array_to_str(warnings),
        }

if __name__=='__main__':

    print(f"Will perform a test")
    do_test=True

    try:
        # Files (need realpath)
        test_path=os.path.realpath('../tests/supplementary_files/')
        def append(file, root_path=[test_path]):
            new_file =os.path.join(*root_path, file)
            assert os.path.isfile(new_file)
            return new_file
        # Actual files
        config_file=append('config_files/test_run_routines_config.yaml')
        fchk_file=append('fchk_files/mp2_sto-3g_H2.fchk.xz')

        # dummy record and other entries
        record={
            'id':'dummy_id',
            'method':'MBIS',
            'fchk_file':fchk_file,
        }
        worker_id='wid-dummy'
        target_dir='dummy_target_dir'

        # Create new target directory
        if os.path.isdir(target_dir):
            p=subprocess.Popen(f"rm -r {target_dir}", shell=True)
            p.communicate()
        os.mkdir(target_dir)
    except Exception as ex:
        raise Exception(f"Error in dummy data preparation: {ex}")

    try:
        exc_partitioning(config_file, record, worker_id, target_dir=target_dir, do_test=do_test)
        print(f"Succesfully performed test. All good")
    except Exception as ex:
        raise Exception(f"Failed to perform test: {ex}")

