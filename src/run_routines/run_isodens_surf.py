#!/usr/bin/env python
from . import *


@validate_call
def run_isodens_surf(python_exc, esp_script, fchk_file: str, record, worker_id, num_threads, target_dir, do_test):
    """ Calculate Grid by python routine """
    tracker=Tracker()

    # Generate jobname and switch to designated work directory
    record_id=record['id']
    jobname=make_jobname(record_id, worker_id)
    work_dir=make_dir(jobname)
    os.chdir(work_dir)

    # Create link to fchk file
    linked_file=f"ln_{os.path.basename(fchk_file)}"
    if os.path.islink(linked_file):
        os.unlink(linked_file)
    p=sp.Popen(f'ln -s {fchk_file} {linked_file}', shell=True)
    p.communicate()

    # Settings values
    isod_key='iso_density'
    spac_key='spacing'
    assert isod_key in record.keys()
    iso_dens_vals=[ record[isod_key] ]
    assert spac_key in record.keys()
    grid_spacings=[ record[spac_key] ]
    # Settings as options
    isod_str='-iso_dens '+' '.join([str(x) for x in iso_dens_vals])
    if grid_spacings==None:
        gs_str=''
    else:
        gs_str='-grid_spacing '+' '.join([str(x) for x in grid_spacings])

    # Excecution
    if tracker.no_error:
        try:
            cmd=f"export OMP_NUM_THREADS={num_threads}; {python_exc} {esp_script} {linked_file} {isod_str} {gs_str}"
            horton=sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, preexec_fn=os.setsid)
            stdout, stderr = horton.communicate()
            if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")
        except Exception as ex:
            tracker.add_error(f"Error in executing horton: {ex}")

    if tracker.no_error:
        # Recover files
        try:
            results_files='results.json'
            assert os.path.isfile(results_files)
            with open(results_files, 'r') as rd:
                content=json.load(rd)
            surface_file=content['files']['grids'][str(iso_dens_vals[0])]
            surf_file={
                'hostname': os.uname()[1],
                'path_to_container': os.path.relpath(os.path.dirname(surface_file),os.environ['HOME'] ),
                'path_in_container': '.',
                'file_name': os.path.basename(surface_file),
            }
            try:
                with open(surface_file, 'r') as rd:
                    data=json.load(rd)
                    meta=data['meta']
                    num_vertices=meta['num_vertices']
                    num_faces=meta['num_faces']
            except Exception as ex:
                raise Exception(f"Problem in reading from {surface_file}: {ex}")

            # Get the spacing
            try:
                isodensities=content['isodensities']
                spacings=content['spacings']
                the_spacing=[ spacings[i] for i, isod in enumerate(isodensities)]
                assert len(the_spacing)==1
                the_spacing=the_spacing[0]
            except Exception as ex:
                raise Exception(f"Error in getting the spacing: {str(ex)}")


            record['spacing']=the_spacing
            record['converged']=1
            record['num_faces']=num_faces
            record['num_vertices']=num_vertices

            run_data={'surface_file':surf_file}
            converged=1
        except Exception as ex:
            converged=0

    record.update({'converged':converged, **tracker.model_dump()})
    run_info={'status':tracker.status,'status_code':tracker.status_code}
    record.update({'run_data':run_data, 'run_info':run_info})
    return record

if __name__=='__main__':
    raise Exception(f"Script execution not yet implemented")