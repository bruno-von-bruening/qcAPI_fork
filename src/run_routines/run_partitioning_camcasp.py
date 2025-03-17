from . import *

@validate_call
def modify_cks(cks_file:file, heavy_Z:int=11):
    params={
        'W-EPS': 0.03, # for aVQZ: Brom is 0.032, and silicon 0.01!
    }

    Lines=open(cks_file,'r').readlines()

    new_Lines=[] ; _basis=False ; _check_sfun=False
    for line in Lines:
        if line.strip().upper().startswith("W-EPS"):
            ar=line.split() ; ar[2]=f"{params['W-EPS']}" ; line=" "*4+" ".join(ar)+'\n'
        elif line.strip().upper().startswith("BASIS AUX") or \
                line.strip().upper().startswith("BASIS ATOM-AUX") :
            _basis=True 
        # In case we are in the basis block
        elif _basis:
            if _check_sfun:
                _check_sfun=False
                if line.strip().upper().startswith("SYMMETRY"):
                    line=f"! {line}"

            if line.strip().upper().startswith("END"):
                _basis=False
            elif line.strip().startswith("#include-camcasp basis/auxiliary/ISA/set2"):
                at=re.search(r'[A-Z][a-z]?',line.split('/')[-1]).group()
                if element_symbol_to_nuclear_charge(at) >=heavy_Z:
                    line=f"! {line}"
                    _check_sfun=True
            elif line.strip().startswith("#include-camcasp basis/auxiliary/aug-cc-pV"):
                at=re.search(r'[A-Z][a-z]?',line.split('/')[-1]).group()
                # For bromine
                if element_symbol_to_nuclear_charge(at)==35:
                    line=6*' '+f"#include-camcasp basis/auxiliary/ISA/test_Bromine/Br\n"
        new_Lines.append(line)
    open(cks_file,'w').writelines(new_Lines)

from typing import Literal

def escape_sequences(input:List[str]):
    mapper={
        '__hyph__':'-',
        '__dot__':'.',
    }
    def has_mod(word):
        if word.endswith('__hash__'):
            return f"#{word.replace('__hash__','')}"
        else:
            return None
    def is_plain(word):
        if word.endswith('__plain__'):
            return f""
        else:
            return None
    
    new_list=[]
    for i in input:
        for k,v in mapper.items():
            i=i.replace(k,v)
        
        for word in i.split():
            if not has_mod(word) is None:
                i=i.replace(word,has_mod(word))
            if not is_plain(word) is None:
                i=i.replace(word,is_plain(word))
        
        new_list.append(i)
    return new_list

class camcasp_global(BaseModel):
    Units: List[str]=['Bohr','Degrees','Hartree']
    Overwrite: Literal['Yes','No']='Yes'
    def make_block(self):
        return escape_sequences(['Global',f"Units {' '.join(self.Units)}", f"Overwrite {self.Overwrite}", 'End'])
class camcasp_molecule(BaseModel):
    NAME: str
    I__dot__P__dot__: float|None =None
    HOMO: float|None = None
    coordinates__plain__: str
    def make_block(self):
        opts=[]
        for x in ['I__dot__P__dot__','HOMO']:
            if not getattr(self, x) is  None:
                opt+=[ f"{x} {getattr(self,x)} a.u."]
        return escape_sequences([f"Molecule {self.NAME}",*opts, self.coordinates__plain__,'End'])
class camcasp_run_type(BaseModel):
    type__plain__: str='Properties'
    Molecule: str
    Main__hyph__Basis: str='aug-cc-pVTZ TYPE MC'
    Aux__hyph__Basis: str='aug-cc-pVTZ   Type  MC   Spherical   Use-ISA-Basis'
    AtomAux__hyph__Basis: str='aug-cc-pVQZ   Type  MC   Spherical   Use-ISA-Basis'
    ISA__hyph__Basis: str='set2   Min-S-exp-H = 0.0'

    Func: str='pbe0'
    Kernel: str='ALDA+CHF'
    SCF__hyph__code: str='psi4'

    File__hyph__Prefix: str
    
    METHOD__hash__: str='isa-A_v1'
    def make_block(self):
        return escape_sequences(['Run-Type']+[ f"{k} {v}" for k,v in self.model_dump().items() ]+['End'])

class camcasp_config(BaseModel):
    jobname: str
    Title: str='Dummy_Title'
    Global: camcasp_global
    Molecule: camcasp_molecule
    Run__hyph__Type: camcasp_run_type
    def __init__(self, jobname=None, *args, molecule_name:str='Dummy_Molecule', **kwargs, ):
        kwargs.update({'jobname':jobname})

        for x in ['Global','Molecule','Run__hyph__Type']:
            if not x in kwargs.keys():
                kwargs.update({x:{}})
        kwargs['Molecule'].update({'NAME':molecule_name})
        kwargs['Run__hyph__Type'].update({'Molecule':molecule_name})
        
        if not 'File__hyph__Prefix' in kwargs['Run__hyph__Type'].keys():
            kwargs['Run__hyph__Type']['File__hyph__Prefix']=jobname
        if not jobname is None:
            kwargs.update({'Title':jobname})

        super().__init__(*args,**kwargs)
    def make_input_file(self, filename:str|None=None):
        """ writes the config into a clt input file  """

        # Care about filename
        if filename is None:
            filename=self.jobname+'.clt'
        filename=filename.replace('.clt','')+'.clt'

        # get all the blocks
        content=[]
        for x in [self.Global, self.Molecule, self.Run__hyph__Type]:
            content+=['']+x.make_block()
        # add the frame of the body
        Body=[f"Title {self.Title}",'',*content, '','Finish']
        content='\n'.join([b.strip('\n') for b in Body])+'\n'

        # Dump to file
        with open(filename, 'w') as wr:
            wr.write(content)
        return filename


@validate_call
def prepare_input(jobname,method,camcasp_path:directory, fchk_file:file, molecule_name:str='The_Molecule'):
    """ """
    executable=os.path.join(camcasp_path, 'bin','runcamcasp.py')
    assert os.path.isfile(executable)

    from qc_global_utilities.environment.file_handling import temporary_file
    fchk_file=temporary_file(fchk_file,copy=True)

    # Get lines in format for CAMCASP coordinates (Tag, Z, Coor)
    camcasp_coordinates=geometry(fchk_file.file).camcasp_coordinates_string

    job_tag='dummy_name'
    if method=='BSISA':
        method_file='isa-A_v1'
    elif method=='GDMA':
        method_file='GDMA'
    else:
        raise Exception(f"Unkown method: {method}")
    out_file=camcasp_config(jobname=job_tag, molecule_name=molecule_name, Molecule={'coordinates__plain__':camcasp_coordinates},
                            Run__hyph__Type={'METHOD__hash__':method_file}
    ).make_input_file()

    # Setup camcasp
    cmd=f"{executable} {job_tag} --setup --ifexists 'delete'"
    stdout,stderr = run_command_in_shell(cmd)
    sitenames=f"{job_tag}/{job_tag}.sites"
    assert os.path.isfile(sitenames),f"Expected file {sitenames} after generating CamCASP setup directory"

    # Read FCHK File
    read_fchk_exc=os.path.join(camcasp_path,'bin','readfchk.py')
    assert os.path.isfile(read_fchk_exc)
    cmd=f"{read_fchk_exc} {fchk_file.file} --prefix {job_tag}/{job_tag}-A --sites {sitenames} --dalton"
    stdout, stderr=run_command_in_shell(cmd)
    # Change the cks file for heavy elements
    cks_file=f"{job_tag}/{job_tag}.cks"
    modify_cks(cks_file, heavy_Z=11) 

    fchk_file.remove_tmp()

    return executable, job_tag

def exc_partitioning_camcasp(camcasp_path, fchk_file, record, worker_id, num_threads=1, max_iter=150, target_dir=None, do_test=False):
    """ """
    tracker=Tracker()
    run_data=None
    try:
        jobname=make_jobname(record['id'], worker_id, job_tag='PART-'+record['method'])
        method=record['method']
        spec=record['Settings']
        assert spec is None, f"You put specifications into your camcasp partitioning for method {method}. Specifications cannot be discerned in this project yet."
        dirname=make_dir(jobname)
        molecule_name='dummy_molecule'
        os.chdir(dirname)
        executable, job_tag=prepare_input(jobname, method, camcasp_path, fchk_file, molecule_name=molecule_name)
    except Exception as ex:
        tracker.add_error(f"Failed in preparing input: {ex}")
    
    if tracker.no_error:
        # Run Camcasp
        try:
            cmd=f"{executable} {job_tag} -d {job_tag} --restart"
            run_command_in_shell(cmd)
        except Exception as ex:
            tracker.add_error(f"Failed running camcasp: {cmd}: {ex}")
    if tracker.no_error:
        try:
            # Output path
            output_path=os.path.join(dirname, job_tag, 'OUT')
            assert os.path.isdir(output_path)
            #
            def get_file(name):
                the_file=os.path.join(output_path, name)
                assert os.path.isfile(the_file), f"Expected solution file does not exist: {the_file}"
                return the_file
        
            from util.util import NAME_BSISA, NAME_LISA , NAME_GDMA , NAME_MBIS 
            
            if method in [NAME_LISA, NAME_BSISA]:
                mom_file=get_file(f"{molecule_name}_ISA-GRID.mom")
                sol_file=get_file(f"{molecule_name}_atoms.ISA")
                sol=molecular_radial_basis(sol_file)
                sol=sol.for_qcAPI()
            elif method in [NAME_GDMA]:
                mom_file=get_file(f"DMA_Z3_L5.mom")
                sol=None
            else: raise Exception(f"Unkown method of tag: {method}")

            mom=multipoles(mom_file).for_qcAPI()

            mom_file_di=dict(
                hostname=os.uname()[1],
                path_to_container= os.path.relpath(os.path.dirname(os.path.realpath(mom_file)) , os.environ['HOME'])  ,
                path_in_container='.',
                file_name=os.path.basename(mom_file),
            )

            run_data={
                'multipoles':mom,
                'solution':  sol,
                'mom_file':  mom_file_di,
            }
        except Exception as ex:
            tracker.add_error(f"Failed recovering results: {analyse_exception(ex)}")
    
    if tracker.no_error:
        converged=1
    else:
        converged=0
    # mul_key='multipoles'
    # sol_key='solution'
    # mom_fi_key='mom_file'
    record.update({'converged':converged,**tracker.model_dump()})
    run_info={'status':tracker.status, 'status_code':tracker.status_code}
    record.update({"run_data":run_data, 'run_info':run_info})

    return record


if __name__=='__main___':
    raise Exception(f"This script cannot  yet be run as main")