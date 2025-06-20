
from . import *

class track_ids(BaseModel):
    succeeded: List[int|str] = []
    failed: List[int|str]= []
    omitted: List[int|str] = []
    prerequisites_not_met: List[int|str]=[]
    @validate_call
    def add_successful(self, item: int|str ):
        self.succeeded+=[ item ]
    @validate_call
    def add_prerequisites_not_met(self, item: int|str ):
        self.prerequisite_not_met+=[ item ]
    @validate_call
    def add_failed(self, item:int|str ):
        self.failed+=[ item ]
    @validate_call
    def add_omitted(self, item:int|str ):
        self.omitted+=[ item ]


class status_tracker(BaseModel):
    messages: List[str]=[]
    errors: List[str]=[]
    warnings: List[str]=[]

    start_time: float
    _time_log : List[Tuple[str,float]]=[]
    _last_time_point: float|None = None
    def __init__(self, *args, **kwargs):
        if not 'start_time' in kwargs.keys():
            kwargs.update({'start_time': time.time()})
        if not '_last_time_point' in kwargs.keys():
            kwargs.update({'_last_time_point': time.time()})
        super().__init__(*args, **kwargs)

    @property
    def message(self):
        return '\n'.join(self.messages)
    @property
    def warning(self):
        return '\n'.join(self.errors)
    @property
    def error(self):
        return '\n'.join(self.warnings)

    @property
    def time_log(self):
        return self._time_log

    @validate_call
    def add_message(self,input:str|List[str]):
        if isinstance(input, list):
            self.messages=self.messages+input
        else:
            self.messages=self.messages+[input]
    @validate_call
    def add_warning(self,input:str|List[str]):
        if isinstance(input, list):
            self.warnings+=input
        else:
            self.warnings.append(input)
    @validate_call
    def add_error(self,input:str|List[str]):
        if isinstance(input, list):
            self.errors+=input
        else:
            self.errors.append(input)

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

class track_http_request(status_tracker):
    @validate_call
    def dump(self):
        return {'message':self.message, 'error': self.error, 'warning':self.warning, 'time_on_server':time.time()-self.start_time, 'time_log':self.time_log}

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

