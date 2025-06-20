
from . import *

from .utils import pdtc_address
from util.requests import get_request

from typing import Union
from sqlmodel.main import SQLModelMetaclass as model_meta
model_pdtc=Union[str|model_meta]

@validate_call
def get_file(address: pdtc_address, object:model_pdtc ,id: str|int, drop_name: str=None, binary:bool|None=None):


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
            assert len(ext)>1, f"File name without extension {fname}!: {ext}"
            ext=ext[-1]
            return ext
        extension=get_extension(fname)
        #assert extension.upper() in ['JSON','YAML'], extension
        if len(os.path.basename(drop_name).split('.'))>1:
            provided_extension=get_extension(drop_name)
            assert extension.upper()==provided_extension.upper(), f"Extension of file found on database and provided output file disagree: databse_extension={extension} provided_extension={provided_extension}"
        else:
            drop_name='.'.join(
                [drop_name]+fname.split('.')[1:]
            )
    except Exception as ex: raise Exception(f"Problem in processing extension: {ex}")
    
    #raise Exception(response.content)

    if binary is None:
        if extension.lower() in ['json','mom','yaml','fchk']: do_decode=True
        elif extension.lower() in ['gz','xz']: do_decode=False
        else:
            default_decode=True
            print(f"Do not know if extension {extension} should be interpreted as binary or not choosing {default_decode}")
            do_decode=default_decode
    else:
        do_decode=binary
    
    try:
        content=response.content
        if do_decode: 
            content=content.decode()
            edit_code='w'
        else:
            edit_code='wb'
        with open(drop_name,edit_code) as wr:
            wr.write(content)
    except Exception as ex: raise Exception(f"Probelm in processing file from content: {ex}")

    return drop_name
    
@validate_call
def get_row(address: pdtc_address, object:model_pdtc, id:List[str|int]|Literal['all']|str|int, links: List[model_pdtc]|None=None,
            #dependencies: List[str]|None=None, merges: List[str]|None=None,
    filters: dict|None=None,         
):
    """ Makes HTTP Request according to the provided arguments and returns the resultof this request"""
    the_object=object if isinstance(object, str) else object.__name__
    links=[ l if isinstance(l, str) else l.__name__ for l in links]
    
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
            opts+=[ f"filters={'--'.join([k,json.dumps(v)])}" ]

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
