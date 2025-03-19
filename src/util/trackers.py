
from . import *

class track_ids(BaseModel):
    succeeded: List[int|str] = []
    failed: List[int|str]= []
    @validate_call
    def add_successful(self, item: int|str ):
        self.succeeded+=[ item ]
    @validate_call
    def add_failed(self, item:int|str ):
        self.failed+=[ item ]
class message_tracker(BaseModel):
    _message: List[str]=[]
    @property
    def message(self):
        return '\n'.join(self._message)
    @validate_call
    def add_message(self,input:str|List[str]):
        if isinstance(input, list):
            self._message+=input
        else:
            self._message.append(input)