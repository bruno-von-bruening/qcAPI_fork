
from . import *

import rdkit.Chem as rdchem
from rdkit.Chem import rdDetermineBonds, rdmolops

def auto_inchi(coordinates, atom_types):
    #https://www.rdkit.org/docs/source/rdkit.Chem.inchi.html
    mol_block=f"{len(coordinates)}\n\n"
    for ty,coor in zip( atom_types, coordinates):
        bohr_to_angstrom=0.529177249
        fac=bohr_to_angstrom
        coor=[f"{float(x)*fac:.8f}" for x in coor]
        mol_block+=f"{ty} {' '.join(coor)}\n"
    rdmol=rdchem.MolFromXYZBlock(mol_block)
    rdDetermineBonds.DetermineBonds(rdmol, charge=0)
    auto_inchi=rdchem.inchi.MolToInchi(rdmol)
    auto_inchi_key=rdchem.inchi.MolToInchiKey(rdmol)

    return auto_inchi, auto_inchi_key

from functools import partial
import os, subprocess
from enum import Enum
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
BOHR = 0.52917721067
ANGSTROM_TO_BOHR=1./BOHR

print_flush = partial(print, flush=True)

def atomic_charge_to_atom_type(Z):
    """ Return atom type when given an atomic Number """
    atom_types=PERIODIC_TABLE_STR.split()
    Z_to_atty_di=dict([ (i+1, at_ty) for i, at_ty in enumerate(atom_types)])
    assert Z in Z_to_atty_di.keys()
    return Z_to_atty_di[Z]

def make_jobname(id, worker_id):
    jobname=f"{id}_wid-{worker_id}"
    return jobname

def make_dir(jobname, base_dir=None ):
    # Make a directory (designated by job name) to run the changes within
    if isinstance(base_dir,type(None)):
        base_dir=os.getcwd()
    check_dir_exists(base_dir)
    target_dir=os.path.join(base_dir,jobname)
    # over write if already exists
    if os.path.isdir(target_dir):
        print(f"Removing directory {target_dir} in order to create a new one")
        p=subprocess.Popen(f"rm -r {target_dir}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        assert p.returncode==0, f"Could not remove directory {target_dir}"

    os.mkdir(target_dir)
    return target_dir

def check_dir_exists(dir):
    hostname=os.uname()[1]
    assert os.path.isdir(dir), f"Cannot find target directory \'{dir}\' in respect to {hostname}:{os.getcwd()}"

def check_response(response):
    status_code=response.status_code

def load_config(config_file):
    assert os.path.isfile(config_file), f"Not a file: {config_file}"
    import yaml
    with open(config_file,'r') as rd:
        config=yaml.safe_load(rd)
    return config
def load_global_config(config_file):
    config=load_config(config_file)
    global_key='global'
    if global_key in config.keys():
        global_conf=config[global_key]
    else:
        print(f"Did not find key {global_key} in {config_file}: {config.keys()}")
        global_conf={}
    return global_conf

def analyse_exception(ex):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file=exc_tb.tb_frame.f_code.co_filename
    line_no=exc_tb.tb_lineno
    return f"{exc_type} {file}:{line_no}:\n{str(ex)}"


