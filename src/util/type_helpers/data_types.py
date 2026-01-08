# in principle that should be the base types of the tables
# They may be parsed from other types


from qcp_orm.tables.tables import Conformation_Base, Wave_Function_Base, Wave_Function
from qcp_objects.objects.properties import geometry
class Conformation_pass(Conformation_Base):
    def __init__(self, *args, **kwargs):
        """ """
        if len(args)==1:
            try:
                geom=geometry(args[0])
            except Exception as ex:
                raise Exception(f"Could not read as xyz files {args[0]}")
            assert not 'coordinates' in kwargs.keys(), f"Double definition of coordinates"
            assert not 'elements' in kwargs.keys(), f"Double definition of elements"
            kwargs.update(
                coordinates=geom.coordinates.tolist(),
                elements=geom.elements,
            )
        elif len(args)>1: raise Exception(f"Exepcted maximum one argument")
        else:
            assert 'coordinates' in kwargs.keys()
            assert 'elements' in kwargs.keys()
    
        super().__init__(**kwargs)

class Wave_Function_pass(Wave_Function):   
    def __init__(self, *args, **kwargs):
        """ Should only use method and basis as mandatory arguments """
        kwargs.update(
            conformation_id='fill_me',
        )

        super().__init__(**kwargs)