import psi4

import numpy as np
import time
import multiprocessing as mp
import os ; from os.path import isfile, isdir
import sys
from functools import partial
import subprocess as sp
import json

import requests
from http import HTTPStatus

# Custom modules
sys.path.insert(1, os.environ['SCR'])
from run_routines.run_psi4_grac import compute_wave_function
from run_routines.run_partitioning import exc_partitioning
import modules.mod_objects as m_obj
import modules.mod_utils as m_utl

from util.util import load_global_config

# Associated (satelite) module
from util.util import atomic_charge_to_atom_type, BOHR, ANGSTROM_TO_BOHR, print_flush

def prepare_wfn_script(config_file, record, serv_adr):
    
    def get_psi4_script(config_file):
        sys.path.insert(1,os.path.realpath('..'))
        global_config=load_global_config(config_file)
        psi4_script=global_config['psi4_script']
        return psi4_script
    def get_geometry(id):
        request_code=f"{serv_adr}/get/geom/{id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get geometry (request={request_code} status_code={status_code}, error={response.text})")
        geom=response.json()
        geom.update({'conf_id':id})

        return geom

    # Get geometry
    conf_id_key='conformation_id'
    assert conf_id_key in record.keys()
    id=record[conf_id_key]
    geom=get_geometry(id)
    psi4_script=get_psi4_script(config_file)
    script=partial(compute_wave_function, psi4_script, geom=geom)
    return script

def prepare_part_script(config_file, record, serv_adr):
    def get_horton_script(config_file):
        sys.path.insert(1,os.path.realpath('..'))
        global_config=load_global_config(config_file)
        horton_key='horton_script'
        assert horton_key in global_config.keys(), f"Cannot find {horton_key} in \'{config_file}\': {global_config}"
        psi4_script=global_config['horton_script']
        return psi4_script

    def get_wave_function_file(id):
        request_code=f"{serv_adr}/get/fchk/{id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get fchk (request={request_code} status_code={status_code}, error={response.text})")
        fchk_info=response.json()

        return fchk_info
    
    def gen_fchk_file(fchk_info):
        target_host=fchk_info['hostname']
        this_host=os.uname()[1]
        assert this_host==target_host, f"File from a different hostname!: here=\'{this_host}\', there=\'{target_host}\'"

        to_storage  =fchk_info['path_to_container']
        to_file     =fchk_info['path_in_container']
        file        =fchk_info['file_name']
        path=os.path.realpath(os.path.join(os.environ['HOME'], to_storage, to_file, file))
        assert os.path.isfile(path), f"Not a existing file: {path}"
        return path
        

    horton_script=get_horton_script(config_file)
    wfn_id=record['wave_function_id']
    fchk_info=get_wave_function_file(wfn_id)
    fchk_file=gen_fchk_file(fchk_info)

    script=partial(exc_partitioning, horton_script, fchk_file)
    return script



def main(config_file, url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
    """ """

    def wait_for_job_completion(res, delay, property):
        """ Check status of job until it changes.
        The job may be done by another worker, then return this info in job_already_done variable"""

        job_already_done = False # Should stay false for regular termination
        while not job_already_done:
            try:
                delay = np.random.uniform(0.8, 1.2) * delay
                entry = res.get(timeout=delay)
                break
            except mp.TimeoutError:
                if property=='wfn':
                    response = requests.get(f"http://{url}:{port}/get_record_status/{record['id']}?worker_id={worker_id}")
                elif property=='part':
                    response = requests.get(f"http://{url}:{port}/get_part_status/{record['id']}?worker_id={worker_id}")
                else:
                    raise Exception()
                if response.status_code != 200:
                    print_flush(
                        f"Error getting record status. Got status code: {response.status_code} , text={response.text}"
                    )
                    continue
                job_status = response.json()
                print_flush("JOB STATUS: ", job_status)
                job_already_done = job_status == 1
        if isinstance(entry, dict):
            
            # Recovering the avaible information
            info_lines=[]
            for key in  ['message','error','warnings']:
                if key in entry.keys():
                    info_lines.append(f"{key.capitalize():<10} : {entry[key]}")
            if len(info_lines)==0:
                info_lines=[f"No information tags where found in the output"]
            
            # Formatting and printing
            info_string=f"Job completition:"
            indent=4*' '
            info='\n'.join([info_string]+ [ indent+x for x in info_lines])
            print(info)

        return entry, job_already_done

    def get_next_record(serv_adr, property='part', method='lisa'):
        """ Get a the next record to be worked at (in case there is none, return none) """
        while True:

            request_code='/'.join([
                    'get_next', property ])
            opts=[('method',method)]
            opts=[ f"{k}={v}" for k,v in opts if v!=None]
            request_code+=f"?{' '.join(opts)}"

            the_request=os.path.join(serv_adr, request_code)
            try:
                response = requests.get(the_request)
                status_code=response.status_code
            except:
                raise Exception(f"Could not communicate with server (address: {serv_adr}, request={request_code}, status_code={status_code})")
            
            # Break because there are no jobs left
            if status_code == HTTPStatus.OK:
                body = response.json()
                record,worker_id = body
                break
            elif status_code == HTTPStatus.NO_CONTENT:
                print_flush("No more records. Exiting.")
                worker_id, record= (None, None)
                break
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
        return record, worker_id

    serv_adr=f"http://{url}:{port}" # address of server
    mp.set_start_method("spawn")

    # Get a new record until there a no one left
    while True:
        # Check for a new record
        record,worker_id=get_next_record(serv_adr, method=method, property=property)
        # Break if nothing to be done anymore
        if isinstance(worker_id, type(None)):
            break

        # Decide which function to use and define arguments
        if property in ['wfn']:
            script=prepare_wfn_script(config_file, record, serv_adr)
        elif property in ['part']:
            script=prepare_part_script(config_file, record, serv_adr)
        else:
            raise Exception()
        args=(record, worker_id)
        kwargs={'do_test':do_test , 'num_threads':num_threads, 'max_iter':max_iter, 'target_dir':target_dir}
        # Start the job
        pool = mp.Pool(1) # Why is this here
        proc = pool.apply_async(script, args=args, kwds=kwargs)
        # Check return of job
        entry, job_already_done =wait_for_job_completion(proc, delay, property)
        
        # In case the has been done by another worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
        else:
            if property in ['wfn']:
                request=f"{serv_adr}/fill/wfn/{worker_id}"
                response = requests.put(request, json=entry)
            elif property in ['part']:
                request=f"{serv_adr}/fill/part/{worker_id}"
                response = requests.put(request, json=entry )
            else:
                raise Exception()
        
        # Check success of request
        status_code=response.status_code
        if status_code == HTTPStatus.OK: # desired
            print(f"Normal Return:\n  Message={response.json()['message']}\n  Error={response.json()['error']}")
            error=None
        elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            error=f"Error in processing"
        elif status_code == HTTPStatus.UNPROCESSABLE_ENTITY: # error in function definition
            error= f"Bad communication with function (check function argument)"
        else:
            error= f"Undescribed error"
        if not isinstance(error, type(None)):
            raise Exception(f"Error updating record ({request}) with code {status_code}: {error}\n{response.text}")
        # Make sure to clean the file system!
        psi4.core.clean()