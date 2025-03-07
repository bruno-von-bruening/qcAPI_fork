from . import *

def analyse_exception(ex):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file=exc_tb.tb_frame.f_code.co_filename
    line_no=exc_tb.tb_lineno
    return f"{exc_type} {file}:{line_no}:\n{str(ex)}"
def my_exception(text:str, exception:Exception) -> None:
    raise Exception(f"{text}:\n{analyse_exception(exception)}")