#!/usr/bin/env python3
import psi4

import argparse
import numpy as np
import time
from functools import partial
import multiprocessing as mp
import os ; from os.path import isfile, isdir

import requests
from brunos_addition import compute_entry_bruno, switch_script
import subprocess
import json
import sys; sys.path.insert(1, os.environ['SCR'])
import modules.mod_objects as m_obj
import modules.mod_utils as m_utl



BOHR = 0.52917721067

PERIODIC_TABLE_STR = """
H                                                                                                                           He
Li  Be                                                                                                  B   C   N   O   F   Ne
Na  Mg                                                                                                  Al  Si  P   S   Cl  Ar
K   Ca  Sc                                                          Ti  V   Cr  Mn  Fe  Co  Ni  Cu  Zn  Ga  Ge  As  Se  Br  Kr
Rb  Sr  Y                                                           Zr  Nb  Mo  Tc  Ru  Rh  Pd  Ag  Cd  In  Sn  Sb  Te  I   Xe
Cs  Ba  La  Ce  Pr  Nd  Pm  Sm  Eu  Gd  Tb  Dy  Ho  Er  Tm  Yb  Lu  Hf  Ta  W   Re  Os  Ir  Pt  Au  Hg  Tl  Pb  Bi  Po  At  Rn
Fr  Ra  Ac  Th  Pa  U   Np  Pu  Am  Cm  Bk  Cf  Es  Fm  Md  No  Lr  Rf  Db  Sg  Bh  Hs  Mt  Ds  Rg  Cn  Nh  Fl  Mc  Lv  Ts  Og
"""

PERIODIC_TABLE = ["Dummy"] + PERIODIC_TABLE_STR.strip().split()

PERIODIC_TABLE_REV_IDX = {s: i for i, s in enumerate(PERIODIC_TABLE)}

print_flush = partial(print, flush=True)
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

    # Check conda
    env_name='horton_wpart'
    python=m_utl.get_conda_env(env_name)

    # Check script
    script=f"{os.environ['SCR']}/execution/run_horton_wpart.py"
    assert isfile(script), f"Not a file: {script}"

    assert 'method' in record.keys()
    method=record['method']
    property='part'
    work_dir=f"{property}_{method}_{record['id']}_{worker_id}"
    os.mkdir(work_dir)
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
        dir_nam='test'
    )

    # Make horton
    input_file, moment_file, solution_file, kld_history_file= make_horton_input_file(rep_di, method, fchk_file=None)
    # Execute Horton
    cmd=f"{python} {script} -inp {input_file}"
    out_file='horton.out'
    horton=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = horton.communicate()
    if horton.returncode!=0: raise Exception(f"The commannd {cmd} did terminate with error: {stderr.decode('utf-8')}")

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
    

    multipoles=dict(
        partitioning_id=record['id'],
        length_units='ANGSTROM',
        representation='CARTESIAN',
        convention='Stone',
        traceless=True,
        multipoles=moms_json,
    )


    record['converged']=1    
    return {
        'part':record,
        'multipoles':multipoles,
    }


    


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
        symbols = [PERIODIC_TABLE[s] for s in species]
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


def main(url, port, num_threads, max_iter, delay, target_dir=None, do_test=False, property='wfn', method=None, fchk_link_file=None):
    """ """

    def wait_for_job(res, delay, property):
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
        worker_id, record= (None, None)
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
            if status_code == 210:
                print_flush("No more records. Exiting.")
                break
            # Succesfully get job
            elif status_code ==200:
                body = response.json()
                record,worker_id = body
                print("WORKER ID: ", worker_id)
                print(record)
                break
            # Communication Error
            elif status_code == 201:
                raise Exception(f"Error from communicating with server: error_code={status_code}, message={response.json()['detail']}")
                break
            # Other errors
            else:
                print_flush(f"Error getting next record command ({request_code}). Retrying in a bit. (received code {status_code}, detail={response.text})")
                time.sleep(0.5)
                continue
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
                script=compute_entry_bruno
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
        res = pool.apply_async(script, args=args)
        # Check return of job
        entry, job_already_done =wait_for_job(res, delay, property)
        
        # In case the has been done by another worker I still want to kill the worker 
        if job_already_done:
            print_flush("Job already done by another worker. Killing QC calculation and getting a new job.")
            pool.terminate()
            pool.join()
            entry = record
                          
        if property in ['wfn']:
            request=f"{serv_adr}/qc_result/{worker_id}"
            response = requests.put(request, json=entry)
        elif property in ['part']:
            request=f"{serv_adr}/fill/part/LISA/{worker_id}"
            response = requests.put(request, json=entry )
            part=entry['part']
            print('PART', part)
        else:
            raise Exception()
        
        # Check success of request
        status_code=response.status_code
        if status_code == 200: # desired
            print(response.json()['message'])
            error=None
        elif status_code == 422: # error in function definition
            error= f"Error update record ({request}: Bad communication with function (check function argument)"
        elif status_code != 200:
            error= f"Error updating record ({request}). Got status code: {response.status_code}, message={response.text}"
        if not isinstance(error, type(None)):
            raise Exception(error)
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

    hostname=os.uname()[1]
    assert os.path.isdir(target_dir), f"Cannot find target directory \'{target_dir}\' in respect to {hostname}:{os.getcwd()}"

    main(url, port, num_threads, max_iter, delay, target_dir=target_dir, do_test=do_test, property=property, method=method)
