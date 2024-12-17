#!/usr/bin/env python3
import psi4

import argparse
import numpy as np
import time
import multiprocessing as mp
import os ; from os.path import isfile, isdir

import requests
from run_routines.run_psi4_grac import compute_entry_grac
from run_routines.run_partitioning import exc_partitioning
import subprocess
import json
import sys; sys.path.insert(1, os.environ['SCR'])
import modules.mod_objects as m_obj
import modules.mod_utils as m_utl

from utility import atomic_charge_to_atom_type, BOHR, ANGSTROM_TO_BOHR, print_flush, check_dir_exists, HTTPcodes

def compute_entry(record, worker_id, num_threads=1, maxiter=150, target_dir=None, do_test=False):
    conformation = record["conformation"]
    method = record["method"]
    basis = record["basis"]
    restricted = record["restricted"]
    id = record["id"]

    start_time = time.time()
    try:

        psi4.set_output_file(f"{id}.out", False)
        psi4.set_options({"scf__maxiter": maxiter})
        if restricted:
            psi4.set_options({'scf__reference': 'RHF'})
        else:
            psi4.set_options({'scf__reference': 'UHF'})
            psi4.set_options({'scf__guess_mix': 'True'})
        psi4.set_options({"scf__wcombine": False})
        psi4.set_num_threads(num_threads)
        # psi4.core.be_quiet()

        # If test is requested run hydrogen molecule
        if do_test:
            coordinates=[[0,0,0],[0,0,1]]
            species=[1,1]
        else:
            coordinates = conformation["coordinates"]
            species = conformation["species"]
        symbols = [ atomic_charge_to_atom_type(s) for s in species]
        total_charge = round(conformation.get("total_charge", 0))
        total_atomic_number = np.sum(species)
        multiplicity = 1 if (total_charge + total_atomic_number) % 2 == 0 else 2

        geom_str = f"{total_charge} {multiplicity}\nnocom\nnoreorient\n"

        for s, (x, y, z) in zip(symbols, coordinates):
            geom_str += f"{s} {x} {y} {z}\n"

        print_flush(geom_str)

        mol = psi4.geometry(geom_str)
        mol.reset_point_group("c1")

        if method == "none" or basis == "none":
            raise ValueError("Method or basis not defined")

        de, wfn = psi4.gradient(f"{method}/{basis}", molecule=mol, return_wfn=True)
        forces = -de.np / BOHR
        e = wfn.energy()
        props = [
            "dipole",
            "wiberg_lowdin_indices",
            "mayer_indices",
            "mbis_charges",
            # "MBIS_VOLUME_RATIOS",
        ]
        psi4.oeprop(wfn, *props, title="test")

        dipole = wfn.array_variable("SCF DIPOLE").np.flatten() * BOHR
        mbis_charges = wfn.array_variable("MBIS CHARGES").np.flatten()
        mbis_dipoles = wfn.array_variable("MBIS DIPOLES").np * BOHR
        mbis_quadrupoles = wfn.array_variable("MBIS QUADRUPOLES").np * BOHR**2
        mbis_octupoles = wfn.array_variable("MBIS OCTUPOLES").np * BOHR**3
        mbis_volumes = (
            wfn.array_variable("MBIS RADIAL MOMENTS <R^3>").np.flatten() * BOHR**3
        )
        # mbis_volume_ratios = wfn.array_variable("MBIS VOLUME RATIOS").np.flatten()
        mbis_valence_widths = (
            wfn.array_variable("MBIS VALENCE WIDTHS").np.flatten() * BOHR
        )
        wiberg_lowdin_indices = wfn.array_variable("WIBERG LOWDIN INDICES").np
        mayer_indices = wfn.array_variable("MAYER INDICES").np

        elapsed_time = time.time() - start_time

        output_conf = dict(
            species=species,
            coordinates=coordinates,
            total_charge=total_charge,
            energy=e,
            forces=forces.tolist(),
            dipole=dipole.tolist(),
            mbis_charges=mbis_charges.tolist(),
            mbis_dipoles=mbis_dipoles.tolist(),
            mbis_quadrupoles=mbis_quadrupoles.tolist(),
            mbis_octupoles=mbis_octupoles.tolist(),
            mbis_volumes=mbis_volumes.tolist(),
            # mbis_volume_ratios=mbis_volume_ratios.tolist(),
            mbis_valence_widths=mbis_valence_widths.tolist(),
            wiberg_lowdin_indices=wiberg_lowdin_indices.tolist(),
            mayer_indices=mayer_indices.tolist(),
        )
        output = dict(
            id=record.get("id", None),
            conformation=output_conf,
            elapsed_time=elapsed_time,
            converged=1,
            method=method,
            basis=basis,
        )
        os.system(f"rm {id}*")


    except Exception as ex:
        print_flush(f"Error computing entry: {ex}")
        elapsed_time = time.time() - start_time
        output = dict(
            id=record.get("id", None),
            conformation=conformation,
            elapsed_time=elapsed_time,
            converged=0,
            method=method,
            basis=basis,
            error=str(ex),
        )

    psi4.core.clean()

    return output

def switch_script(record):
    """ Run either my our the original entry calculation script"""
    method=record['method']
    if method.upper()=='PBE0-GRAC':
        return 'bruno'
    else:
        return 'original'

def main(url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
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

        return entry, job_already_done

    def get_next_record(serv_adr, property='part', method='lisa'):
        """ Get a the next record to be worked at (in case there is none, return none) """
        while True:
            # Determine type of request
            assert not isinstance(property, type(None))
            if method in ['',None]:
                request_code='/'.join([
                    'get_next_record', property ])
            else:
                request_code='/'.join([
                    'get_next_record', property, method ])

            response = requests.get(f"{serv_adr}/{request_code}")
            status_code=response.status_code
            # Break because there are no jobs left
            if status_code == HTTPcodes.normal:
                body = response.json()
                record,worker_id = body
                break
            elif status_code == HTTPcodes.escape:
                print_flush("No more records. Exiting.")
                worker_id, record= (None, None)
                break
            elif status_code== HTTPcodes.internal_error:
                error=f"Internal Error"
                raise Exception(f"{error} ({request}): (received code {status_code}, detail={response.text})")
            else:
                eror=f"Unkown Error"
                print(f"{error} ({request}): Retrying in a bit. (received code {status_code}, detail={response.text})")
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
            mode=switch_script(record)
            if mode=='bruno':
                script=compute_entry_grac
            elif mode=='original':
                script=compute_entry
            else:
                raise Exception(f"Switch function failure (invalid return): {mode}")
        elif property in ['part']:
            script=exc_partitioning
        else:
            raise Exception()
        args=(record, worker_id, num_threads, max_iter, target_dir, do_test)
        # Start the job
        pool = mp.Pool(1) # Why is this here
        proc = pool.apply_async(script, args=args)
        # Check return of job
        entry, job_already_done =wait_for_job_completion(proc, delay, property)
        
        # In case the has been done by another worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
                          
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
        if status_code == 200: # desired
            print(f"Job : {response.json()['message']} with error message {response.json()['error']}")
            error=None
        elif status_code == HTTPcodes.internal_error:
            error=f"Error in processing"
        elif status_code == 422: # error in function definition
            error= f"Bad communication with function (check function argument)"
        elif status_code != 200:
            error= f"Undescribed error"
        if not isinstance(error, type(None)):
            raise Exception(f"Error updating record ({request}) with code {status_code}: {error}\n{response.text}")
        # Make sure to clean the file system!
        psi4.core.clean()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate a qcAPI dataserv_adr with jobs")
    parser.add_argument(
        "address",
        type=str,
        default="127.0.0.1:8000",
        help="URL:PORT of the qcAPI server",
    )
    parser.add_argument(
        "--num_threads", "-n", type=int, default=1, help="Number of threads to use"
    )
    parser.add_argument(
        "--maxiter", "-m", type=int, default=150, help="Maximum number of SCF iterations"
    )
    parser.add_argument(
        "--delay", "-d", type=float, default=60, help="Ping frequency in seconds"
    )
    parser.add_argument(
        '--target_dir', type=str, help='where to save file', default=os.getcwd()
    )
    parser.add_argument(
        '--test', action='store_true', help='run test using hydrogen molecule and excepting when error is encountered in psi4 run'
    )
    parser.add_argument(
        '--do_lisa', action='store_true', help='run lisa job'
    )
    parser.add_argument(
        '--fchk_link', type=str, help='file where fchk_files are stored',
    )
    parser.add_argument(
        '--property', type=str
    )
    parser.add_argument(
        '--method' , type=str
    )
    parser.add_argument(
        '--basis',   type=str
    )
    

    args = parser.parse_args()
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    target_dir=args.target_dir
    do_test=args.test
    num_threads=args.num_threads
    max_iter=args.maxiter
    delay=args.delay

    # filter by property and specs
    property    = args.property
    method      = args.method
    basis       = args.basis

    check_dir_exists(target_dir)

    main(url, port, num_threads, max_iter, delay, target_dir=target_dir, do_test=do_test, property=property, method=method)
