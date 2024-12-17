
from functools import partial
import os, subprocess
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
