import pubchempy as pcp
from . import *
from qcp_orm.tables.tables import Compound_Base

@val_call
def load_compounds_from_pubchem(cids:List[int]=[], inchikeys:List[str]=[]):
    """ Loads pubchem information and returns information object """
    """ 3d or 2d info doesnt make much time difference ( test for a list of 96 was 2 secs, single took 0.3 s!) """
    num_cids=len(cids)
    num_inchis=len(inchikeys)
    assert sum([ num_cids, num_inchis])>0, f"Did not provide any keys!"

    c=[]
    if num_cids>0:
        comp_py_cid = pcp.get_compounds(cids, 'cid', record_type='2d')
        c+=comp_py_cid
        
    if num_inchis>0:
        comp_by_inchi = pcp.get_compounds(inchikeys, 'inchikey', record_type='2d')
        for inchi in inchikeys:
            matches=[ x for x in comp_by_inchi if x.inchikey==inchi ]
            if len(matches)==0:
                raise Exception(f"Could not find compound with inchi={inchi} in pubchem")
            elif len(matches)==1:
                c+=matches
            else:
                match_objects=sorted([ (x,pubchem_handler(input=x).cid) for x in matches ],
                                     key=lambda x: x[1] )
                warn(f"Multiple matches found for inchikey={inchi} in pubchem, taking the one with lowest cid={match_objects[0][1]}"
                     f" (available cids={[ x[1] for x in match_objects ]})"
                )
                c+= [ match_objects[0][0] ]

    return c
class pubchem_handler(BaseModel):
    isomeric_smiles:str|None=None
    canonical_smiles:str|None=None
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

        comp=Compound_Base(**kwargs)
        return comp.model_dump()
