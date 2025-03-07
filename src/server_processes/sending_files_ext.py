
from . import *

# Solution https://stackoverflow.com/questions/60716529/download-file-using-fastapi
from starlette.responses import FileResponse
from property_database.data_models.utilities import File_Model
from data_base.utils import table_mapper

def get_file_from_table(the_class):
    #assert issubclass(the_class)
    assert hasattr(the_class,'path'), f"The object: {the_class} has not attribute \'path\'"
    path=the_class.path
    assert os.path.isfile(path)
    return path

def file_response(file_path):
    response = FileResponse(path=os.path.realpath(file_path), filename=os.path.basename(file_path))
    return response

@validate_call
def get_file_table(file_type:str) -> SQLModel:
    """ Should handle any classes that are subclasses of base type """
    file_class=table_mapper(file_type)
    assert issubclass(file_class, File_Model)
    return file_class