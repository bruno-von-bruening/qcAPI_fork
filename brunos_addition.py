
# NEW
import os
import sys
from run_psi4_2 import complete_calc, psi4_exit_ana, property_calc
import subprocess
import time
import psi4
import numpy as np
from functools import partial

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
    atom_types=PERIODIC_TABLE_STR.split()
    Z_to_atty_di=dict([ (i+1, at_ty) for i, at_ty in enumerate(atom_types)])
    assert Z in Z_to_atty_di.keys()
    return Z_to_atty_di[Z]

def run_psi4(atom_types, coordinates, dft_functional, basis_set, jobname='test'):
    psi4_start_time=time.time()
    psi4_dict=complete_calc(atom_types, coordinates, dft_functional=dft_functional, basis_set=basis_set, jobname=jobname, units={'LENGTH':'BOHR'})  
    psi4_input_file=psi4_dict['psi4_input_file']
    psi4_wafe_function=psi4_dict['wfn_file']

    subprocess.run(["psi4", psi4_input_file]) 
    psi4_dict.update({'exit_ana': psi4_exit_ana(psi4_dict['output_file']),
        'real_time':time.time()-psi4_start_time,
        'time_unit':'s'})

    return psi4_dict
    
def extract_psi4_info(wfn_file, method, basis):
    psi4_dict=property_calc(wfn_file, method, basis)



def compute_entry_new(record, num_threads=1, maxiter=150):
    
    start_time = time.time()

    conformation = record["conformation"]
    conformation={'species':[1,1], 'coordinates':[[0,0,0],[0,0,1]]}
    method='PBE0'
    basis='aug-cc-pVTZ'
    restricted=True
    id = record["id"]
    jobname=f"{id}"
    #try:

    dft_functional='PBE0'
    basis_set='aug-cc-pVTZ'

    atom_types=[ atomic_charge_to_atom_type(x) for x in conformation['species']]
    coordinates=np.array(conformation['coordinates'])*ANGSTROM_TO_BOHR

    try:
        psi4_dict = run_psi4(atom_types, coordinates, jobname=jobname, dft_functional=dft_functional, basis_set=basis_set)

        output_conformation=extract_psi4_info(psi4_dict['wfn_file'], dft_functional, basis_set)

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
        method=method,
        basis=basis,
        error=error,
    )
    return output


    #except Exception as ex:
    #    print_flush(f"Error computing entry: {ex}")
    #    elapsed_time = time.time() - start_time
    #    output = dict(
    #        id=record.get("id", None),
    #        conformation=conformation,
    #        elapsed_time=elapsed_time,
    #        converged=0,
    #        method=method,
    #        basis=basis,
    #        error=str(ex),
    #    )


    return output

