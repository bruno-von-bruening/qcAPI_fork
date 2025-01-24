#!/usr/bin/env python

import sys, os ; sys.path.insert(1, os.environ['SCR'])
import subprocess
import json
import modules.mod_utils as m_utl
from utility import make_jobname, make_dir

def run_isodens_surf(record, worker_id, num_threads, target_dir, do_test):
    """ Calculate Grid by python routine """
    error=None

    # Generate jobname and switch to designated work directory
    record_id=record['id']
    jobname=make_jobname(record_id, worker_id)
    work_dir=make_dir(jobname)
    os.chdir(work_dir)

    # Check prerequisites
    env_name='density_evaluations'
    python=m_utl.get_conda_env(env_name)
    python_script=os.path.join(os.environ['SCR'], 'property_calc' , 'calc_IsoDensSurf.py')
    assert os.path.isfile(python_script)
    assert 'fchk_file' in record.keys() # manipulated!
    fchk_fi=record['fchk_file']
    assert os.path.isfile(fchk_fi)

    # Create link to fchk file
    linked_file=f"ln_{os.path.basename(fchk_fi)}"
    if os.path.islink(linked_file):
        os.unlink(linked_file)
    p=subprocess.Popen(f'ln -s {fchk_fi} {linked_file}', shell=True)
    p.communicate()

    # Settings values
    isod_key='isodensity_value'
    assert isod_key in record.keys()
    iso_dens_vals=[ record[isod_key] ]
    grid_spacings=None
    # Settings as options
    isod_str='-iso_dens '+' '.join([str(x) for x in iso_dens_vals])
    if grid_spacings==None:
        gs_str=''
    else:
        gs_str='-grid_spacing '+' '.join([str(x) for x in grid_spacings])

    # Excecution
    if error==None:
        try:
            cmd=f"export OMP_NUM_THREADS={num_threads}; {python} {python_script} {linked_file} {isod_str} {gs_str}"
            horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            stdout, stderr = horton.communicate()
            if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")
        except Exception as ex:
            error=f"Error in executing horton: {ex}"

    if error==None:
        # Recover files
        try:
            results_files='results.json'
            assert os.path.isfile(results_files)
            with open(results_files, 'r') as rd:
                content=json.load(rd)
            surface_file=content['files']['grids'][str(iso_dens_vals[0])]

            # Get the spacing
            try:
                isodensities=content['isodensities']
                spacings=content['spacings']
                the_spacing=[ spacings[i] for i, isod in enumerate(isodensities)]
                assert len(the_spacing)==1
                the_spacing=the_spacing[0]
            except Exception as ex:
                raise Exception(f"Error in getting the spacing: {str(ex)}")


            record['surface_file']=surface_file
            record['spacing']=the_spacing
            record['converged']=1
        except Exception as ex:
            error=f"Error in recovering results: {ex} {content}"

    message='Successful Execution'
    return {
        'surface':record,
        'message':message,
        'error':error,
    }

if __name__=='__main__':
    raise Exception(f"Script execution not yet implemented")