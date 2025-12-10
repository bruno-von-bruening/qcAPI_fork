
from . import *
@val_cal
def parse_dict(dictionary:List[str]):
    try:
        new_dictionary={}
        for x in dictionary:
            sep='--'
            ar=x.split(sep)
            assert len(ar)==2, f"Expected separator \'{sep}\' in dictionary option but got {x}"
            new_dictionary.update({ar[0]:json.loads(ar[1])})
        return new_dictionary
    except Exception as ex: my_exception(f"Could not read as dicitonary", ex)

