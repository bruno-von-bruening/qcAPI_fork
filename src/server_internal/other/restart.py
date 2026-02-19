from . import *

from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse

import threading
restart_flag = threading.Event()


class RestartRequest(Exception):
    """Silent restart trigger for supervisor loop."""
    def __init__(self, message=None):
        self.message = message or "Server restart requested"

    def __str__(self):
        return self.message


def trigger_restart(server):
    server.should_exit = True
    #restart_flag.set()
    print(">>> [RESTART] Restart requested by client")
    #raise Exception()
    # raise Exception('test')
    # print("Triggering server restart...")
    # raise RestartRequest("Restart requested by client")

def add_restart_function(app, SessionDep):
    # @app.exception_handler(RestartRequest)
    # async def restart_exception_handler(request: Request, exc: RestartRequest):
    #     # Print a neat message to server log
    #     print(f">>> [RESTART] {exc}")
    #     # Return a clean JSON response to the client
    #     raise Exception()
    @app.post("/restart_server")
    async def restart(
        background_tasks: BackgroundTasks,
    ):
        """Raise server restart signal to main server process"""
        server=app.state.server
        background_tasks.add_task(trigger_restart, server)
        return {"message":"Restart signal sent to server"}
        # background_tasks.add_task(trigger_restart)
