
from . import *

import rdkit.Chem as rdchem
from rdkit.Chem import rdDetermineBonds, rdmolops

from .auxiliary import my_exception, analyse_exception
from typing import Literal

from functools import partial
import os, subprocess
from enum import Enum

from qcp_global_utils.encoding_and_conversion.encoding import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from qcp_global_utils.encoding_and_conversion.constants import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR

FAVICON_KEY='QCAPI_FAVICON'

# The unique names:
NAME_COMP       ='compound'
NAME_CONF       ='conformation'
NAME_WFN        ='wave_function'
NAME_WFN_FILE   ='FCHK_File'
NAME_MOM_FILE   ='MOM_File'
NAME_PART       ='partitioning'
NAME_IDSURF     ='isodensity_surface'
NAME_ESPRHO     ='density_esp'
NAME_ESPDMP     ='multipolar_esp'
NAME_ESPCMP     ='compare_esp'
NAME_ESPCMP_FILE='espcmp_file'
NAME_GROUP      ='group'
NAME_DISPOL     ='distributed_polarisabilities'
NAME_PAIRPOL_FILE ='pairwise_polarisabilities_file'
NAME_WFN_DAT    ='wave_function_run_data'
NAME_PART_DAT   ='hirshfeld_partitioning_run_data'
NAME_DISPOL_DAT   ='distributed_polarisabilities_run_data'
def make_name_dict():
    # Additional names added to key iteslf
    names={
        NAME_CONF:[],
        NAME_WFN:['wfn'],
        NAME_PART:['part'],
        NAME_IDSURF: ['isosurf'],
        NAME_ESPRHO: ['esprho'],
        NAME_ESPDMP: ['espdmp'],
        NAME_ESPCMP: ['espcmp'],
        NAME_GROUP: [],
        NAME_COMP: [],
        NAME_DISPOL: ['dispol'],
        NAME_PAIRPOL_FILE: [],
        NAME_WFN_FILE: [],
        NAME_MOM_FILE: [],
        NAME_WFN_DAT: [],
        NAME_PART_DAT: [],
        NAME_DISPOL_DAT: [],
    }
    # Names for functions, key will be added to list
    [ names[k].append(k) for k in names.keys()]
    return names
names=make_name_dict()

OP_DELETE       = 'delete'
OP_CLEAN_DOUBLE       = 'clean_double'
OP_CLEAN_PENDING    ='clean_pending'
OP_RESET        = 'reset'
available_operations=[ OP_DELETE , OP_CLEAN_DOUBLE, OP_CLEAN_PENDING, OP_RESET]

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

def make_available_properties(names: dict) -> List[float]:
    avail_prop=[]
    for k,v in names.items():
        avail_prop +=[k]+list(v) 
available_properties=make_available_properties(names)

@validate_call
def get_unique_tag(object:str, print_options: bool =False)-> str:
    def do_print_options():
        lines=[f"Following options are accepted:"]
        indent=4*' '
        max_leng=max([ len(the_key) for the_key in names.keys() ])
        for the_key, aliases in names.items():
            aliases_key=','.join(aliases)
            the_key=the_key+' '*(max_leng-len(the_key))
            lines+=[f"{indent}- {the_key} ( aliases={aliases_key} )"]
        return '\n'.join(lines)
        
    # Get a unique name for the object
    object=object.lower()
    found_tags=[]
    for prop, tags in names.items():
        if object in [x.lower() for x in tags]:
            found_tags.append(prop)
    if len(found_tags)!=1:
        if not print_options:
            raise Exception(f"Option {object} cannot be interpreted\n"+do_print_options())
        else:
            quit(f"Option {object} cannot be interpredted\n"+do_print_options())
    else:
        object_tag=found_tags[0]
    return object_tag


print_flush = partial(print, flush=True)

@validate_call
def make_upper(string:str):
    return string.upper()
part_method_choice=Annotated[ Literal['MBIS','LISA','GDMA','BSISA'], BeforeValidator(make_upper)]


@validate_call
def make_jobname(id: int|str, worker_id: str, job_tag: str=None):
    """Generates name of job with provided id, the worker id and an optional prefix tag """
    jobname=f"{id}_wid-{worker_id}"
    if not isinstance(job_tag, type(None)):
        jobname='_'.join([job_tag, jobname])
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





def check_address(address):
    import requests
    try:
        response=requests.get(address)
    except Exception as ex:
        raise Exception(f"Cannot communicate with address ({address}):\n {ex}")

