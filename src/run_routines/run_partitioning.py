from . import *
import numpy as np

#import modules.mod_objects as m_obj
from qc_objects.objects.property import multipoles as obj_multipoles
from os.path import isfile, isdir
import subprocess, json, shutil, glob
import numpy as np
from util.util import analyse_exception


def prepare_input(tracker, worker_id, record, horton_script, fchk_file):

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
            tracker.add_warning(f"Resorting to default value of horton script but should be designated in config file.")
            script=f"{os.environ['SCR']}/execution/run_horton_wpart.py"
        if isinstance(script, type(None)): raise Exception(f"Failed in finding horton script")
        assert isfile(script), f"Not a file: {script}"

        return script
    def prepare_horton_arguments(src_fchk_file):

        # Get fchk_file and link it
        file=src_fchk_file; assert isfile(file), f"FCHK file in record is not valid: {file}"
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
        tmp_fchk_file=linked_file
        method=record['method']
        basis=record['basis']
        return jobname, method, basis, rep_di, tmp_fchk_file
    def make_horton_input_file(rep_di, method, basis, fchk_file):
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
            'BASIS' : basis,
        }
        input_file= f"INPUT_{rep_di['dir_nam']}.inp"
        with open(input_file, 'w') as wr:
            for k in inp_di.keys():
                wr.write(f"{k:<20} {inp_di[k]:}\n")
        return input_file, moment_file, solution_file, kld_history_file
    

    try:
        # Get environment dependent python and run script
        script = horton_script#get_script(config_file)
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
        jobname, method, basis, rep_di, tmp_fchk_file= prepare_horton_arguments(fchk_file)
        assert os.path.isfile(tmp_fchk_file)
    except Exception as ex:
        raise Exception(f"Error while preparing horton input arguments: {ex}")


    try:
        # construct horton input file
        input_file, moment_file, solution_file, kld_history_file= make_horton_input_file(rep_di, method, basis, tmp_fchk_file)
    except Exception as ex:
        raise Exception(f"Error while generating horton input file: {ex}")
    
    return tracker, script, input_file, jobname, work_dir

####
def execute_horton(tracker, python, script, input_file, num_threads=1):
    # Execute Horton
    output_file=input_file.split('.')[0]+'.hrtout'
    error_file=input_file.split('.')[0]+'.hrterr'
    
    cmd=f"export OMP_NUM_THREADS={num_threads}; {python} {script} -inp {input_file} 1> {output_file} 2> {error_file}"
    horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = horton.communicate()

    with open(error_file, 'r') as rd:
        horton_stderr=rd.readlines()
    indent=4*' '
    horton_stderr=[ indent+'|-'+x.strip() for x in horton_stderr ]
    horton_stderr='\n'.join(horton_stderr)

    error_message=f"The commannd \'{cmd}\' did terminate with error: stderr={stderr.decode('utf-8')}\nhorton_stderr={horton_stderr}"
    if horton.returncode!=0: raise Exception(error_message)

    return tracker

def recover_horton_results(tracker, record, jobname, work_dir, target_dir):
    def load_results_overview():
        """ horton execution drops a files that contains all the location of the generated file 
        add the absolute path to this dir (files are defined relative to this)
        """
        results_file='results.json'
        assert os.path.isfile(results_file)
        with open(results_file, 'r') as rd:
            results=json.load(rd)
        results.update({'location':os.path.realpath(os.path.dirname(results_file)), 'the_file':results_file})
        return results

    def get_moments(results):
        try:
            mom_file=results['files']['moments']
            mom_file=os.path.join( results['location'],mom_file )
        except Exception as ex:
            raise Exception(f"Cannot read moments in {results['the_file']}: {ex}")
        assert isfile(mom_file), f"{mom_file}"
        
        try:
            mom= obj_multipoles(mom_file)
            moms= mom.get_moments
            ranks=mom.ranks_list
            moms=np.array(moms).tolist()
            moms_json=json.dumps(moms)
        except Exception as ex:
            raise Exception(f"Error in reading moment from file {mom_file} with {obj_multipoles}: {analyse_exception(ex)}")
        return mom_file, ranks,moms_json
    def get_solution(results):
        try:
            file_tag='files'
            sol_tag='solution'
            sol_file=results[file_tag][sol_tag]

            sol=molecular_radial_basis(sol_file)
            sol=sol.for_qcAPI()

            return sol
            
        except Exception as ex:
            raise Exception(f"Failure in getting solution fomr {results['the_file']}: {ex}")
    def get_kld(results):
        try:
            file_tag='files'
            kld_tag='kld_history'
            kld_file=results[file_tag][kld_tag]

            assert os.path.isfile(kld_file)
            with open(kld_file, 'r') as rd:
                lines=rd.readlines()
                lines=[ l.strip() for l in lines]
                lines=[ l for l in lines if not l.startswith('#')]
                kld_his=[]
                for l in lines:
                    try:
                        kld_his.append(float(l))
                    except Exception as ex: my_exception(f"Cannot convert to float ({l})", ex)
            kld=kld_his[-1]
            return kld
        except Exception as ex: my_exception(f"Problem in recovering kld:",ex)

    results=load_results_overview()



    def copying_results(tracker):
        try:
            os.chdir('..')
            if os.path.realpath(target_dir)!=os.getcwd():
                target=make_dir(jobname, base_dir=target_dir)
                assert os.path.isdir(target)
                source_files= glob.glob(f"{work_dir}/*")
                assert len(source_files)>0, f"No output files in {os.path.realpath(work_dir)}!"
                [ shutil.move(file, target) for file in source_files ]
                os.rmdir(work_dir)
            else:
                target=jobname
        except Exception as ex:
            tracker.add_error(f"Problems in copying results: {str(ex)}")
        return tracker, target

    results=load_results_overview()
    mom_file,ranks,moms_json=get_moments(results)
    solution=get_solution(results)
    kld=get_kld(results)
    ranks='|'.join([' '.join(['']+ [str(y) for y in x]+['']) for x in ranks] )

    # copy the results to target directory and append errors if encountered
    tracker, target_dir=copying_results(tracker)
    new_mom_file=os.path.join( target_dir, os.path.basename(mom_file) )
    assert os.path.isfile(new_mom_file)
    mom_fi_di={
        'hostname':os.uname()[1],
        'path_to_container': os.path.relpath(os.path.dirname(new_mom_file), os.environ['HOME']),
        'path_in_container': '.',
        'file_name':os.path.basename(new_mom_file)
    }

    multipoles=dict(
        id=record['id'],
        length_units='ANGSTROM',
        representation='CARTESIAN',
        convention='Stone',
        ranks=ranks,
        traceless=True,
        multipoles=moms_json,
    )
    return tracker, mom_fi_di, multipoles,solution, kld

def exc_partitioning(python_exc, horton_script, fchk_file, record, worker_id, num_threads=1, max_iter=150, target_dir=None, do_test=False):
    
    # Create warnings and errors that get change permanently in the exception blog
    tracker=Tracker()
    
    try:
        # Input generation
        try:
            tracker, script, input_file, jobname, work_dir = prepare_input(tracker, worker_id, record, horton_script, fchk_file)
        except Exception as ex: my_exception(f"Error in preparing data:", ex)

        # Execution            
        try:
            tracker=execute_horton(tracker,python_exc, script, input_file,num_threads=num_threads)
        except Exception as ex: my_exception(f"Error in executing horton:", ex)

        # Recovering Results
        try:
            tracker, mom_file, multipoles, solution, KLD=recover_horton_results(tracker, record, jobname, work_dir, target_dir)
        except Exception as ex: my_exception(f"Error in postprocessing horton run :", ex)
        
        record.update({'KLD':KLD})
        converged=1    
        run_data={
            'multipoles':multipoles,
            'mom_file': mom_file,
            'solution':solution,
        }
    except Exception as ex:
        if do_test:
            raise Exception(f"Run failed with error: {ex}")
        else:
            print(f"Run failed with error: {str(ex)}")
        run_data=None
        converged=0
    
    record.update({'converged':converged, **tracker.model_dump()})
    run_info={'status':tracker.status, 'status_code':tracker.status_code}
    record.update({'run_data':run_data, 'run_info':run_info})
    return record

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

