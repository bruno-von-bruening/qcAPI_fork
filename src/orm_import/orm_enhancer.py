from . import *

from qcp_orm.linked_tables.linked_tables import Molecular_Multipoles
from qcp_objects.objects.properties import MolecularMultipoleMoments 


# The sqlalchemy/ sqlmodel classes are a bit fragile, do not inherit them!

@classmethod
def from_object(cls,  *args, **kwargs):
    if len(args)==1:
        if isinstance(args[0],MolecularMultipoleMoments):
            mom=args[0]
            assert mom.symmetrized, f"Expected symmetrized {MolecularMultipoleMoments.__name__} object but got non-symmetrized one."
            try:
                new_kwargs=dict(
                    convention=('Cartesian' if mom.representation=='C' else 1/0 ),
                    expansion_center_type=mom.type_of_center,
                    expansion_center=json.dumps([ float(x) for x in mom.expansion_center ]),
                    multipoles=json.dumps( [ float(x) for x in mom.tensor_elements ] ),
                    ranks=json.dumps( [ int(x) for x in mom.ranks ] ),
                )
            except Exception as ex:
                raise Exception(f"{args[0]}: {ex}")

            for k,v in new_kwargs.items():
                if k in kwargs:
                    assert kwargs[k]==v, f"Conflict in argument {k} passed to Molecular_Multipoles constructor: {kwargs[k]} (given) vs {v} (from MolecularMoments object)."
                kwargs[k]=v
        else:
            raise Exception(f"Positional argument passed to Molecular_Multipoles constructor is not of type MolecularMoments but {type(args[0])}.")
    elif len(args)>1:
        raise Exception(f"More than one positional argument ({len(args)}) passed to Molecular_Multipoles constructor. Only zero or one allowed.")
    
    return cls(**kwargs)
Molecular_Multipoles.from_object = classmethod(from_object)