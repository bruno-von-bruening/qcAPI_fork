from . import *

def operation_functions(app, SessionDep):
    @app.post("/delete/{prop}")
    async def delete(
        prop: str,
        session: SessionDep,
    ):
        try:
            the_object=object_mapper[prop]
            entries= session.exec( select(the_object) ).all()
            for entry in entries:
                session.delete(entry)
            session.commit()
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not execute delete for {prop}: {analyse_exception(ex)}")

