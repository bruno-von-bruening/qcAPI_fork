from . import *
from .import_helper import *


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

class Tracker_data(myBaseModel):
    messages    : my_list=[]
    warnings    : my_list=[]
    errors      : my_list=[]
    time_start  : float|None=None
    time_end    : float|None=None


@validate_call
def make_jobname(id: int|str, worker_id: str, job_tag: str=None):
    """Generates name of job with provided id, the worker id and an optional prefix tag """
    jobname=f"{id}_wid-{worker_id}"
    if not isinstance(job_tag, type(None)):
        jobname='_'.join([job_tag, jobname])
    return jobname

class Tracker_data(Tracker_data):
    worker_id: str # Should be inherited maybe
    main_record_id: str|int
    job_name: str # Should be inherited maybe
    server_address: str
    num_threads: int | None = Field(default=None, gt=0, description="Number of threads to be used in the calculation")
    memory_GB: float|None=None
    target_dir: str|None=None
    test: bool=False
    config_file: pdtc_file|None=None
    working_dir: str
    record_type: SQLModelMetaclass

    def __init__(self,*args,**kwargs):

        make_auto_jobname=True
        if 'job_name' in kwargs.keys():
            if not kwargs['job_name'] in ['auto',None]:
                make_auto_jobname=False

        if make_auto_jobname:
            kwargs.update({'job_name':'auto'})

        auto_working_dir=True
        if 'working_dir' in kwargs.keys():
            if not kwargs['working_dir'] in ['auto',None]:
                auto_working_dir=False
        if auto_working_dir:
            kwargs['working_dir']='auto'

        super().__init__(*args,**kwargs)
        self.job_name=f"{self.record_type.__name__}_{self.main_record_id}_wid-{self.worker_id}"
        self.working_dir=os.path.join(self.target_dir, self.job_name)





class Tracker(Tracker_data):
    def __init__(self, *args, **kwargs):
        time_start=time.time()
        super().__init__(*args, **kwargs, time_start=time_start)
    def model_dump(self,**kwargs):
        if self.time_end is None:
            self.time_end=time.time()
        dic=super().model_dump(**kwargs)
        dic.update({'elapsed_time':self.time_end-self.time_start})
        dic.pop('time_end',None)
        dic.pop('time_start',None)
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
