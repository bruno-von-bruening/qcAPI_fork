import pubchempy as pcp
from . import *

@val_call
def load_compounds_from_pubchem(cids:List[int]=[], inchikeys:List[str]=[]):
    """ Loads pubchem information and returns information object """
    """ 3d or 2d info doesnt make much time difference ( test for a list of 96 was 2 secs, single took 0.3 s!) """
    num_cids=len(cids)
    num_inchis=len(inchikeys)
    assert sum([ num_cids, num_inchis])>0, f"Did not provide any keys!"

    c=[]
    if num_cids>0:
        c += pcp.get_compounds(cids, 'cid', record_type='2d')
    if num_inchis>0:
        c += pcp.get_compounds(inchikeys, 'inchikey', record_type='2d')

    return c
class pubchem_handler(BaseModel):
    isomeric_smiles:str
    canonical_smiles:str
    iupac_name: str|None=None
    molecular_formula: str

    cid: int
    inchi: str
    inchikey: str
    synonyms: List[str]
    
    charge: int
    elements: List[str]
    molecular_weight: float|None=None
    bonds: List[dict]
class pubchem_handler(pubchem_handler):
    def __init__(self,input=None,**kwargs):
        if not input is None:
            assert isinstance(input, pcp.Compound)
            interesting_keys=['isomeric_smiles', 'canonical_smiles', 'charge','elements','bonds','iupac_name',
            'molecular_formula','molecular_weight','synonyms', 'cid','inchikey','inchi']
            tmp=time.time()
            kwargs=input.to_dict(properties=interesting_keys)
            #dic.update(input.to_dict(properties=['aids','synonyms','sids']))
            #keys_not_there=[k for k in interesting_keys if k not in dic.keys()]
            #if len(keys_not_there)>0: raise Exception(keys_not_there)
            #kwargs.update(dict([
            #    (k, dic[k]) for k in interesting_keys
            #]))
        super().__init__(**kwargs)
    def to_database_entry(self):
        # Get that into shape of compound_base
        keys=['charge','elements','bonds','molecular_formula','isomeric_smiles','inchi','inchikey','iupac_name','molecular_weight']
        self_di=self.model_dump()
        
        kwargs=dict(
            source='pubchem', comments=None, multiplicity=1
        )
        kwargs.update(dict([
            (k,self_di[k]) for k in keys
        ]))

        from qcp_database.tables import Compound_Base
        comp=Compound_Base(**kwargs)
        return comp.model_dump()