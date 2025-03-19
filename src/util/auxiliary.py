from . import *

def analyse_exception(ex):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file=exc_tb.tb_frame.f_code.co_filename
    line_no=exc_tb.tb_lineno
    return f"{exc_type} {file}:{line_no}:\n{str(ex)}"
def my_exception(text:str, exception:Exception) -> None:
    raise Exception(f"{text}:\n{analyse_exception(exception)}")

class message_tracker(BaseModel):
    _message: List[str]=[]
    start_time: float
    _time_log : List[Tuple[str,float]]=[]
    _last_time_point: float|None = None
    def __init__(self, *args, **kwargs):
        if not '_start_time' in kwargs.keys():
            kwargs.update({'start_time': time.time()})
        if not '_last_time_point' in kwargs.keys():
            kwargs.update({'_last_time_point': time.time()})
        super().__init__(*args, **kwargs)

    @property
    def message(self):
        return '\n'.join(self._message)
    @property
    def time_log(self):
        return self._time_log
    
    @validate_call
    def add_message(self,input:str|List[str]):
        if isinstance(input, list):
            self._message+=input
        else:
            self._message.append(input)

    @validate_call
    def start_timing(self):
        self._last_time_point=time.time()
    @validate_call
    def stop_timing(self, name:str, timing:float|None=None):
        current_time=time.time()
        if timing is None:
            timing = current_time - self._last_time_point
        self._last_time_point=current_time

        self._time_log.append( (name, timing) )

    @validate_call
    def dump(self):
        return {'message':self.message, 'elapsed_time':time.time()-self.start_time, 'time_log':self.time_log}