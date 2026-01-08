from . import *


from .body import (
    get_url_func,
    post_populate,
    process_arguments,
)

from .populate_extension import (
    load_pubchem_data,
)
from util.requests import make_url
from qcp_objects.objects.properties import geometry

from util.util import exit
def open_file(f):
    try:
        data=load_json_or_yaml(f)
        return data
    except Exception as ex:
        exit(f"Could not process file \'{f}\' as json or yaml file:\n{ex}")

class MolPol_data(myBaseModel):
    class Config:
        extra='forbid'
        validate_assignment = True
    ids: List[str]|Literal['all']='all' # wfn ids
    records: List[Molecular_Polarizability]=[ Molecular_Polarizability(blank=True) ]
    # def create_records(self)-> List[dict]:
    #     pass
    # def model_dump(self,**kwargs)-> dict:
    #     dic= super().model_dump(**kwargs)
    #     dic['approach']= self.approach if isinstance(self.approach, (Molecular_Polarizability.allowed_approaches,str)) else [ x for x in self.approach ]
    #     return dic
    def model_dump(self,**kwargs)-> dict:
        dic= super().model_dump(**kwargs)
        dic['records']= [ x.model_dump() for x in self.records ]
        return dic

@val_call
def get_kwargs_MolPol(files:List[pdtc_file]):
    data = [load_json_or_yaml(f) for f in files]
    if len(data)>1:
        raise NotImplementedError(f"Implement merging of multiple MolPol data files.")
    elif len(data)==0:
        checked_d=MolPol_data()
    else:
        try:
            checked_d=MolPol_data(**(data[0]))
        except Exception as ex:
            error=f"Could not process data from {files}:\n{ex}"
            dummy_file=f"dummy_molpol_data.yaml"
            with open(dummy_file,'w') as wr:
                yaml.safe_dump(MolPol_data().model_dump(), wr)
            error+=f"\nWrote dummy file as reference to \'{dummy_file}\'"
            raise Exception(error)
    kwargs=checked_d.model_dump()

    return kwargs

@val_call
def main(
    filenames:List[file_pdtc],address, 
    prop:sqlmodel_meta|str, 
    method:str|None=None, 
    basis:str|None=None, 
    do_test=False
):
    """ Switch dependant on which property to compute"""
    if isinstance(prop, str):
        prop=get_object_for_tag(prop)

    content, content_file= ( process_arguments(filenames)
                            if len(filenames)>0 else (None, None) )
    func=get_url_func(prop)

    
    if prop==Compound:
        inchikey_tag='inchi_keys'
        from .populate_extension import load_pubchem_data
        if not content is None:
            assert inchikey_tag in content.keys(), f"Expected key \'{inchikey_tag}\' in \'{content_file}\'"
            inchi_keys=content[inchikey_tag]
        else: 
            exit(f"Provide a file in which you dropped a dictionary with key \'{inchikey_tag}\' that holds a list of inchikeys")

        compounds=load_pubchem_data(inchi_keys)
        kwargs=dict(records=compounds)
    elif prop==Conformation:
        records=[]
        if content is None:
            exit(f"No content provided, nothing todo.")
        elif isinstance(content, list):
            records+=content
        elif isinstance(content, dict):
            assert 'records' in content.keys(), f"Expected \'records\' in \'{content_file}\'"
            assert all( isinstance(x, dict) for x in content['records'] ), f"Expected list of dictionaries in \'records\' in \'{content_file}\'"
            records+=content['records']


        inchi_map={}
        rec_ref=[]
        for rec in records:
            try:
                rec_ref+=[ Conformation(**rec) ]
            except Exception as ex: 
                required_keys=['inchikey','geometry']
                cnt=sum([ k in rec.keys() for k in required_keys ])
                if cnt==len(required_keys):
                    geom=geometry(rec['geometry'])
                    geom.units.LENGTH='BOHR'
                    coords=geom.coordinates.reshape(-1)
                    elements=geom.atom_types
                    
                    geom.units.LENGTH='ANGSTROM'
                    inchi, inchi_key=auto_inchi(geom.coordinates, elements)
                    # inchi, inchi_key=auto_inchi(geom.coordinates, elements)
                    if 'inchikey' in rec.keys():
                        if rec['inchikey'].lower() == 'auto':
                            pass
                        else:
                            if inchi_key!=rec['inchikey']:
                                warn(f"Provided inchikey \'{rec['inchikey']}\' does not match generated inchikey \'{inchi_key}\' (inchi={inchi}) from geometry through rdkit (very usual) !")
                                inchi_key=rec['inchikey']

                    rec_ref+=[ Conformation(
                        compound_id=inchi_key,
                        coordinates=coords, elements=elements,
                    )]
                    inchi_map.update({ inchi_key: inchi })

                else: raise Exception(f"Could not generate record for {rec}: {ex}")
        # inchis=[ r.compound_id for r in rec_ref ]
        from receiver.get_request import get_row
        entries=get_row(address, 'compound', ids=list(set(inchi_map.keys())) )
        existing_inchis=[ r[get_primary_key_name(Compound)] for r in json.loads(entries['json'])['record'] ]
        missing_inchis=[ x for x in inchi_map.keys() if x not in existing_inchis ]

        from .populate_extension import load_pubchem_data
        if len(missing_inchis)>0:
            compounds=load_pubchem_data(missing_inchis, inchi_mapper=inchi_map)


            opts, json_content= get_url_func(Compound)(records=compounds)
            request_body=f"populate/{get_unique_tag(Compound).lower()}"
            request_code=make_url(address, request_body, opts)
            print(f"Posting request code: {request_code}")

            post_populate(request_code, json=json_content)

        #json_content=dict(records=[x.model_dump() for x in rec_ref])        
        kwargs=dict(records=[x.model_dump() for x in rec_ref])
        
    elif prop==Wave_Function:
        #assert all([ os.path.isfile(x) for x in filenames ])
        #from util.type_helpers.data_types import Wave_Function_pass
        # filenames=[]

        if content is not None:
            try:
                assert isinstance(content, list), f"Expected list of level_of_theory entries in file \'{content_file}\'"
                assert all( isinstance(x, dict) for x in content), f"Expected list of dictionaries in file \'{content_file}\'"
                lots= [ Wave_Function(**x, blank=True) for x in content ]
            except Exception as ex: raise Exception(f"Could not process content of file \'{content_file}\' as {Wave_Function}:\n{ex}")
        else:
            lots=[]
        
        cnt=sum([ x is not  None for x in [method,basis] ])
        if cnt==0:
            pass
        elif cnt==1:
            exit(f"Provided only method or basis set but both needed to make wave function level of theory.")
        else: # cnt==2
            lots+=[ Wave_Function(method=method, basis=basis)]
        kwargs=dict(records=[ x.model_dump() for x in lots],conf_ids='all')
    elif prop==Hirshfeld_Partitioning:
        kwargs=dict(method=method, basis=basis)
    elif prop==IsoDens_Surface:
        kwargs={}
    elif prop==RHO_ESP_Map:
        kwargs={}
    elif prop==DMP_ESP_Map:
        kwargs={}
    elif prop==DMP_vs_RHO_ESP_Map:
        kwargs={}
    elif prop==Group:
        kwargs=dict(content_file=content_file)
    elif prop==Distributed_Polarisabilities:
        kwargs={}
    elif prop==Molecular_Polarizability:
        kwargs=get_kwargs_MolPol(filenames)
    else:
        raise Exception(f"No case implemented for handling property {property}")

    
    opts, json_content= func(**kwargs)
    request_body=f"populate/{prop.__name__}"
    request_code=make_url(address, request_body, opts)
    print(f"Posting request code: {request_code}")

    post_populate(request_code, json=json_content)
