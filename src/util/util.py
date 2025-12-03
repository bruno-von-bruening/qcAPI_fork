
from . import *

import rdkit.Chem as rdchem
from rdkit.Chem import rdDetermineBonds, rdmolops

from .auxiliary import my_exception, analyse_exception
from typing import Literal, Union, Dict, List, Tuple

from functools import partial
import os, subprocess
from enum import Enum

from qcp_global_utils.encoding_and_conversion.encoding import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from qcp_global_utils.encoding_and_conversion.constants import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR
from qcp_global_utils.pydantic.pydantic import file as pdtc_file, directory as pdtc_directory

FAVICON_KEY='QCAPI_FAVICON'


OP_DELETE       = 'delete'
OP_CLEAN_DOUBLE       = 'clean_double'
OP_CLEAN_PENDING    ='clean_pending'
OP_RESET        = 'reset'
OP_DROP_TABLE  = 'drop_table'
AVAILABLE_OPERATIONS=[ OP_DELETE , OP_CLEAN_DOUBLE, OP_CLEAN_PENDING, OP_RESET, OP_DROP_TABLE]

NAME_BSISA='BSISA'
NAME_LISA='LISA'
NAME_GDMA='GDMA'
NAME_MBIS='MBIS'


def auto_inchi(coordinates, atom_types):
    #https://www.rdkit.org/docs/source/rdkit.Chem.inchi.html
    mol_block=f"{len(coordinates)}\n\n"
    for ty,coor in zip( atom_types, coordinates):
        fac=BOHR_TO_ANGSTROM
        coor=[f"{float(x)*fac:.8f}" for x in coor]
        mol_block+=f"{ty} {' '.join(coor)}\n"
    rdmol=rdchem.MolFromXYZBlock(mol_block)
    rdDetermineBonds.DetermineBonds(rdmol, charge=0)
    auto_inchi=rdchem.inchi.MolToInchi(rdmol)
    auto_inchi_key=rdchem.inchi.MolToInchiKey(rdmol)

    return auto_inchi, auto_inchi_key




print_flush = partial(print, flush=True)

@validate_call
def make_upper(string:str):
    return string.upper()
part_method_choice=Annotated[ Literal['MBIS','LISA','GDMA','BSISA'], BeforeValidator(make_upper)]



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






from util.environment import directory, file
@validate_call
def link_file(source:file, target:directory=os.getcwd()):
    link_file='ln_'+os.path.basename(source)
    cop=sp.Popen(f"ln -s {source} {link_file}", shell=True)
    cop.communicate()
    return link_file

@validate_call
def copy_file(
    source:file, link:bool=False, target:directory=os.getcwd()
) -> file:
    """ Copy source to target, if link provided instead of copying link"""
    if os.path.realpath(os.path.dirname(source))==os.path.realpath(target):
        return source
    else:
        if link:
            copied_file=link_file(source, target)
        else:
            copied_file=os.path.basename(source)
            shutil.copy(source, os.path.join(target,copied_file))
        return copied_file




