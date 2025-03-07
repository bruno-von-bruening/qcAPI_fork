from . import *


def list_to_string(the_list):
    return json.dumps(the_list)
my_list=Annotated[ List[str], [], PlainSerializer(list_to_string) ]


def process_status(self, code=False):
    if self.no_error and self.no_warning:
        ret=(f"Run finished normally",1)
    elif self.no_error and not self.no_warning:
        ret=(f"Run finished with warning(s)",2)
    elif not self.no_error:
        if self.no_warning:
            ret=(f"Run finished with error(s)",0)
        else:
            ret=(f"Run finished with error(s) and warning(s)",0)
    else:
        raise Exception(f"Forgot case")

    if code:
        return ret[1]
    else:
        return ret[0]
class Tracker_data(BaseModel):
    messages    : my_list=[]
    warnings    : my_list=[]
    errors      : my_list=[]
    time_start  : float|None=None
    time_end    : float|None=None

class Tracker(Tracker_data):
    def __init__(self, *args, **kwargs):
        time_start=time.time()
        super().__init__(*args, **kwargs, time_start=time_start)
    def model_dump(self):
        if self.time_end is None:
            self.time_end=time.time()
        dic=super().model_dump()
        dic.update({'elapsed_time':self.time_end-self.time_start})
        del dic['time_end']
        del dic['time_start']
        return dic

    def add_message(self, x):
        self.messages.append(x)
    def add_warning(self, x):
        self.warnings.append(x)
    def add_error(self, x):
        self.errors.append(str(x))
    @property
    def no_error(self):
        return len(self.errors)<1
    @property
    def no_warning(self):
        return len(self.warnings)<1
    @property
    def status(self):
        return process_status(self, code=False)
    @property
    def status_code(self):
        return process_status(self, code=True)
    
    def get_status(self):
        dic=self.model_dump()
        dic.update({'status':self.status, 'status_code':self.status_code})
        return dic
