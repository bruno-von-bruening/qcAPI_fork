
from . import *

from .utils import pdtc_address
from util.requests import get_request


@validate_call
def get_file(address: pdtc_address, object:str ,id: str|int, drop_name: str=None):


    the_object=object
    if drop_name is None:
        drop_name=f"{object}_{id}"
    
    request_code=f"{address}/get_file/{the_object}?ids={id}"
    response=requests.get(request_code)
    status_code=response.status_code
    if status_code!=HTTPStatus.OK:
        raise Exception(f"{status_code}: {response.text}")

    # Get name of file
    try:
        d = response.headers['content-disposition']
        fname = re.findall("filename=(.+)", d)[0].strip('\'').strip('\"')
    except Exception as ex: raise Exception(f"Cannot get filename from response: {ex}")

    # Get the extension of the file and if provided filename has an extension verify that they are identical
    try:
        def get_extension(fname):
            ext=fname.split('.')
            assert len(ext)>1, f"Found more than one extension!: {ext}"
            ext=ext[-1]
            return ext
        extension=get_extension(fname)
        #assert extension.upper() in ['JSON','YAML'], extension
        if len(os.path.basename(drop_name).split('.'))>1:
            provided_extension=get_extension(drop_name)
            assert extension.upper()==provided_extension.upper(), f"Extension of file found on database and provided output file disagree: databse_extension={extension} provided_extension={provided_extension}"
        else:
            drop_name=drop_name+f".{extension.lower()}"
    except Exception as ex: raise Exception(f"Problem in processing extension: {ex}")
    
    #raise Exception(response.content)

    with open(drop_name,'w') as wr:
        wr.write(response.content.decode())
        #json.dump(json.loads(response.content),wr)
    return drop_name
    
@validate_call
def get_row(address: pdtc_address, object:str, id:List[str|int]|Literal['all']|str|int, links: List[str|int]|None=None,
            #dependencies: List[str]|None=None, merges: List[str]|None=None,
    filters: dict|None=None,         
):
    """ Makes HTTP Request according to the provided arguments and returns the resultof this request"""
    the_object=object
    
    if not isinstance(id, list):
        id=[id]

    request_code=f"{address}/get/{the_object}"

    opts=[ f"ids={i}" for i in id]
    #if not dependencies is None:
    #    request_code+=''.join([ 
    #        f"&deps={dep}" for dep in dependencies
    #    ])
    #if not merges is None:
    #    request_code+=''.join([
    #        f"&merge={m}" for m in merges
    #    ])
    if not links is None:
        opts+=[ f"links={m}" for m in links ]
    if not filters is None:
        for k,v in filters.items():
            opts+=[ f"filters={'--'.join([k,str(v)])}" ]

    if len(opts)>0:
        request_code+='?'+'&'.join(opts)
    return get_request(request_code).json()
    
#@validate_call
#def get_depencencies(address: pdtc_address, object:str, dependenices: List[str]):

@validate_call
def get_group_tree(address: pdtc_address):
    """ """
    request_code=f"{address}/get/group_tree"
    return get_request(request_code).json()['json']