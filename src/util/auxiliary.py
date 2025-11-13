from . import *

def analyse_exception(ex):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file=exc_tb.tb_frame.f_code.co_filename
    line_no=exc_tb.tb_lineno
    return f"{exc_type} {file}:{line_no}:\n{str(ex)}"
def my_exception(text:str, exception:Exception) -> None:
    raise Exception(f"{text}:\n{analyse_exception(exception)}")


@validate_call
def indent(text:List[str]|str,indent_length:int=4, indent_character=' ', line_length=120):
    import textwrap
    the_indent=indent_length * indent_character
    break_length=line_length-len(the_indent)

    # We allways start with one block of text
    if isinstance(text, list):
        text='\n'.join(text)

    # break to lon lines 
    broken_text='\n'.join([ '\n'.join(textwrap.wrap(line,width=break_length)) for line in text.split('\n')])
    # indent text
    indented_text=textwrap.indent(broken_text, the_indent)

    return indented_text