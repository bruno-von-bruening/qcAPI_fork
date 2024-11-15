
# NEW
import os
import sys
from run_psi4_2 import complete_calc, psi4_exit_ana, property_calc
import subprocess
import time
import psi4
import numpy as np
from functools import partial

# Replacers for Thomas script
PERIODIC_TABLE_STR = """
H                                                                                                                           He
Li  Be                                                                                                  B   C   N   O   F   Ne
Na  Mg                                                                                                  Al  Si  P   S   Cl  Ar
K   Ca  Sc                                                          Ti  V   Cr  Mn  Fe  Co  Ni  Cu  Zn  Ga  Ge  As  Se  Br  Kr
Rb  Sr  Y                                                           Zr  Nb  Mo  Tc  Ru  Rh  Pd  Ag  Cd  In  Sn  Sb  Te  I   Xe
Cs  Ba  La  Ce  Pr  Nd  Pm  Sm  Eu  Gd  Tb  Dy  Ho  Er  Tm  Yb  Lu  Hf  Ta  W   Re  Os  Ir  Pt  Au  Hg  Tl  Pb  Bi  Po  At  Rn
Fr  Ra  Ac  Th  Pa  U   Np  Pu  Am  Cm  Bk  Cf  Es  Fm  Md  No  Lr  Rf  Db  Sg  Bh  Hs  Mt  Ds  Rg  Cn  Nh  Fl  Mc  Lv  Ts  Og
"""
ANGSTROM_TO_BOHR=1./0.52917721067

print_flush = partial(print, flush=True)

def atomic_charge_to_atom_type(Z):
    """ Return atom type when given an atomic Number """
    atom_types=PERIODIC_TABLE_STR.split()
    Z_to_atty_di=dict([ (i+1, at_ty) for i, at_ty in enumerate(atom_types)])
    assert Z in Z_to_atty_di.keys()
    return Z_to_atty_di[Z]

def switch_script(record):
    """ Run either my our the original entry calculation script"""
    method=record['method']
    if method.upper()=='PBE0-GRAC':
        return 'bruno'
    else:
        return 'original'


def run_psi4(atom_types, coordinates, dft_functional, basis_set, jobname='test', hardware_settings=None, do_grac=False):
    """ Interface between this data and my generic run script """
    psi4_start_time=time.time()

    # Generate the input file
    psi4_dict=complete_calc(
        atom_types, coordinates, 
        dft_functional=dft_functional, do_grac=do_grac, basis_set=basis_set, 
        jobname=jobname, units={'LENGTH':'BOHR'}, hardware_settings=hardware_settings
    )  
    psi4_input_file=psi4_dict['psi4_input_file']

    # Run the process
    process = subprocess.Popen(
        ['psi4', psi4_input_file],
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    error_code =process.returncode

    # Recover the data
    psi4_dict.update({
        'exit_ana': psi4_exit_ana(psi4_dict['output_file']),
        'real_time':time.time()-psi4_start_time,
        'time_unit':'s',
        'error_code':error_code,
        'stdout':stdout,
        'stderr': stderr})

    return psi4_dict
    
def extract_psi4_info(wfn_file, method, basis):
    """ Interface with generic run script """
    psi4_dict=property_calc(wfn_file, method, basis)
    return psi4_dict

def process_method(record):
    """ Read the method tag """

    method=record['method']
    basis_set=record['basis']
    
    # Parse the method for GRAC key
    method_components=method.split('-')
    if len(method_components)==1:
        dft_functional=method
        do_GRAC=False
    elif len(method_components)==2:
        assert method_components[1].upper()=='GRAC'
        dft_functional=method_components[0]
        do_GRAC=True
    else: 
        raise Exception()
    
    return dft_functional, do_GRAC, basis_set

def compute_entry_bruno(record, num_threads=1, maxiter=150):
    """ Cal psi4 calculation """
    start_time = time.time()

    conformation = record["conformation"]
    atom_types=[ atomic_charge_to_atom_type(x) for x in conformation['species']]
    coordinates=np.array(conformation['coordinates'])*ANGSTROM_TO_BOHR
    # atom_types=['H', 'H']
    # coordinates=[ [0, 0, 0],[1, 0, 0] ]

    id = record["id"]
    jobname=f"{id}"

    dft_functional, do_GRAC, basis_set=process_method(record)

    hardware_settings=dict(
        num_threads=4,
        memory='8GB',
    )

    try:
        # Get the wave function
        psi4_dict = run_psi4(
            atom_types, coordinates, 
            dft_functional=dft_functional, basis_set=basis_set, do_grac=do_GRAC,
            jobname=jobname, hardware_settings=hardware_settings)
        if psi4_dict['error_code']!=0:
            raise Exception(f"Psi4 wave function run did terminate with error: {psi4_dict['stderr']}")

        # Get derived information
        output_conformation=extract_psi4_info(psi4_dict['wfn_file'], dft_functional, basis_set)

        # Get some basis data in
        output_conformation.update(dict(
            species=record['species'],
            coordinates=record['coordinates'],
            total_charge=0,
        ))

        converged=1
        error=None
    except Exception as ex:
        print_flush(f"Error computing entry: {ex}")
        converged=0
        error=str(ex)
        output_conformation=conformation

    elapsed_time=time.time()-start_time
    output = dict(
        id=record.get("id", None),
        conformation=output_conformation,
        elapsed_time=elapsed_time,
        converged=converged,
        method=dft_functional,
        basis=basis_set,
        error=error,
    )
    return output
