from . import *
from .get_main import get_object, get_group_structure
from util.sql_util import *

def get_functions(app, SessionDep):

    @app.get("/get/{object}")
    async def get(
        session     : SessionDep,
        object      : str,
        links       : List[str]=Query([]),
        filters     : List[str]=Query([]),
        ids         : List[str|int]=Query(None),
    ):

        messanger=message_tracker()
        if object in ['group_structure','group_tree']:
            try:
                messanger,return_di=get_group_structure(session,messanger)
            except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Error in recovering Group structure: {analyse_exception(ex)}")
        else:
            messanger.start_timing()
            # Parse filters into dictionary
            try:
                filters=parse_dict(filters)
            except Exception as ex: raise HTTPException(HTTPStatus.BAD_REQUEST, f"Filters argument has error: {analyse_exception(ex)}\nfilters={filters}")
            # Checks
            try: 

                # Check table
                object_table=get_object_for_tag(object)
                the_link_tabs    = [ get_object_for_tag(y) for y in links ]

                # Check ids
                if ids is [] or ids is None:
                    raise Exception(f"Provided empty ids!")
                elif 'all' in ids:
                    ids=session.exec(
                        select( get_primary_key(object_table))
                    ).all()
            except Exception as ex: raise HTTPException(HTTPStatus.BAD_REQUEST, f"Probelm in preprocessing provided argument: {analyse_exception(ex)}")        
            messanger.stop_timing('Preparation')

            try:
                messanger, return_di=get_object(session, messanger, object_table, ids, filters=filters, links=links)
            except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, analyse_exception(ex))

        try:
            return {**messanger.dump(), 'json':return_di}
        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Problem in returin data: {analyse_exception(ex)}")

    @app.get("/get_status/{property}/{id}")
    async def get_status(id: str, property: str, session: SessionDep, worker_id: str = None):
        try:
            if worker_id is not None:
                # update worker timestamp
                worker = session.get(Worker, uuid.UUID(worker_id))
                if worker is not None:
                    worker.timestamp = datetime.datetime.now().timestamp()
                    session.add(worker)
                    session.commit()
            the_object=object_mapper[ get_unique_tag(property) ]
            record = session.get(the_object, id)
                    
            return record.converged
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, analyse_exception(ex))

    @app.get("/get_next/{property}")
    async def get_next(
        session: SessionDep, 
        request: Request,
        property: str,
        method: str = None,
        for_production: bool=True,
    ):
        # Retrieve the record
        try:
            record, worker_id=create_new_worker(session,request,property, method, for_production=for_production)
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, detail=f"Could not get next entry: {str(ex)}")
        
        # Check if record is empty
        if isinstance(record, type(None)):
            raise HTTPException(HTTPStatus.NO_CONTENT, detail=f"No more record to process!")
        else:
            return record, worker_id
    
    @app.get("/get_file/{file_type}")
    async def get_file( session: SessionDep,
        file_type:str,
        ids: List[str|int]=Query([]),  
    ) -> file_response:
        """ Returns a file 
        In case no errors are found
        """


        # Provide info which objects are available
        if file_type=='info':
            available_tables=get_all_available_tables(session)
            available_tables=[ t.__name__ for t in available_tables if issubclass(t, File_Model) ]
            raise HTTPException(HTTPStatus.IM_A_TEAPOT, 
                                '\n'.join( ["Available tables:"]+[f'    - {x}' for x in available_tables ])
            )
        # 
        else:
            # Give some explicit help in case no ids are provided
            if len(ids)==0:
                raise HTTPException(HTTPStatus.BAD_REQUEST, f"Please provide ids through \'?ids=<single id>\' or \'?ids=<first id>&ids=<second id>...\'")
            try:
                the_model=get_object_for_tag(file_type)
                assert issubclass(the_model, File_Model), f"Chosen model {the_model.__name__} is not a subclass of {File_Model.__name__}"
            except Exception as ex:
                raise HTTPException(HTTPStatus.BAD_REQUEST, f"Provided file type argument (\'{file_type}\') could not be recognized: {analyse_exception(ex)}")
        
        try:
            try:
                objects=[session.get(the_model, id) for id in ids]
                files=[ get_file_from_table(x) for x in objects ]
            except Exception as ex: my_exception(f"Problem when recovering files from files:", ex )
        except Exception as ex: raise HTTPException( HTTPStatus.INTERNAL_SERVER_ERROR, str(ex))
            
        if len(files) == 0:
            raise HTTPException(HTTPStatus.UNPROCESSABLE_ENTITY, f"No file found for ids={ids}")
        else:
            try:
                return [ file_response(file) for file in files ][0]
            except Exception as ex: my_exception(f"Error when trying to return files:", ex)
    
    return app
