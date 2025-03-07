from . import *
from .sending_files_ext import get_file_table, get_file_from_table, file_response

from pydantic import Field
from fastapi import Query
def file_functions(app, SessionDep): 

    @app.get("/get_file/{file_type}")
    async def get_file( session: SessionDep,
        file_type:str,
        ids: List[str|int]=Query([]),  
    ):
        try:
            file_table=get_file_table(file_type)
        except Exception as ex:
            raise HTTPException(HTTPStatus.BAD_REQUEST, f"Provided file type argument (\'{file_type}\') could not be recognized: {analyse_exception(ex)}")
        
        try:
            try:
                objects=[session.get(file_table, id) for id in ids]
                files=[ get_file_from_table(x) for x in objects ]
            except Exception as ex: my_exception(f"Problem when recovering files from files:", ex )
            
            try:
                return [ file_response(file) for file in files ][0]
            except Exception as ex: my_exception(f"Error when trying to return files:", ex)
        except Exception as ex: raise HTTPException( HTTPStatus.INTERNAL_SERVER_ERROR, str(ex))

    return app