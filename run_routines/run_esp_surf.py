#!/usr/bin/env python

import sys, os ; sys.path.insert(1, os.environ['SCR'])
import subprocess
import json
import shutil, glob
import modules.mod_utils as m_utl
from utility import make_jobname, make_dir
import numpy as np

def run_esp_surf(record, worker_id, num_threads, target_dir, do_test):
    """ """
    error=None

    # Generate jobname and switch to designated work directory
    record_id=record['id']
    jobname=f"ESP_{make_jobname(record_id, worker_id)}"
    work_dir=make_dir(jobname)
    os.chdir(work_dir)

    # Check prerequisites
    env_name='density_evaluations'
    python=m_utl.get_conda_env(env_name)
    python_script=os.path.join(os.environ['SCR'], 'property_calc' , 'calc_esp.py')
    assert os.path.isfile(python_script)
    assert 'fchk_file' in record.keys() # manipulated!
    fchk_fi=record['fchk_file']
    assert os.path.isfile(fchk_fi)
    grid_fis=[record['grid_file']]
    for grid_fi in grid_fis:
        assert os.path.isfile(grid_fi)

    # Create link to fchk file
    linked_file=f"ln_{os.path.basename(fchk_fi)}"
    if os.path.islink(linked_file):
        os.unlink(linked_file)
    p=subprocess.Popen(f'ln -s {fchk_fi} {linked_file}', shell=True)
    p.communicate()
    # the calculation is interfaced with fortran, hence we need to copy library file (or could also reference as hardcoded path)
    for x in ['.f90','.F90', '.so', '.sh']:
        [ shutil.copy(y,'.') for y in glob.glob(os.path.dirname(python_script)+f"/*{x}")  ]
    # Copy the grid_file
    for grid_fi in grid_fis:
        shutil.copy(grid_fi,'.')

    # Run
    if error==None:
        try:
            grid_fis_str=' '.join(grid_fis)
            cmd=f"export OMP_NUM_THREADS=1; {python} {python_script} {linked_file} {grid_fis_str} ; "
            horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            stdout, stderr = horton.communicate()
            if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")
        except Exception as ex:
            error=f"Error in executing horton: {ex}"
    
    # Recover results
    if error==None:
        try:
            results_files='results.json'
            assert os.path.isfile(results_files)

            with open(results_files, 'r') as rd:
                content=json.load(rd)
            try:
                esp_maps=content['files']['esp-maps']
                if len(esp_maps)!=1:
                    raise Exception(f"Expceted exactely one esp map, found {esp_maps}")
                esp_map=list(esp_maps.values())[0]
            except Exception as ex:
                raise Exception(f"Error in recovering esp maps: {str(ex)}")
            
            record['esp_surface_file']=esp_map
            with open(esp_map,'r') as rd:
                content=json.load(rd)
                scalars_bytes=np.array( content['surface_values'], dtype=np.float32 ).tobytes()
                with open('test.bytes','wb') as wr:
                    wr.write(scalars_bytes)
            record['converged']=1
            message='Successful Execution'
        except Exception as ex:
            raise Exception(f"Error in recovering results: {str(ex)}")
    
    # Copy to target_dir
    if error==None:
        try:
            new_dir=os.path.join(target_dir, os.path.basename(work_dir))
            os.chdir('..')
            assert os.path.isdir(work_dir)
            cmd=f"cp -r {work_dir} {new_dir}"
            copy_dir=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            stdout, stderr = copy_dir.communicate()
            message+=" and successful copying"
            if copy_dir.returncode!=0:
                raise Exception(f"Error in copying {stderr.decode()}")
        except Exception as ex:
            error=f"Error in copying files: {ex} {stderr.decode()}"
            message+=' but failed copying'
            raise Exception(ex)
    
    return {
        'surface':record,
        'message':message,
        'error':error,
    }
    

    