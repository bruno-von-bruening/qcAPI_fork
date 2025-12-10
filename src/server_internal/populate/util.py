from . import *

class counter(BaseModel): # watch out with uninitialized (Dont wanna use enum since values change)
    requested               :int = 0
    prerequisites_not_met   :int = 0
    doubly_requested        :int = 0
    populated               :int = 0
    already_there           :int = 0
    failed                  :int = 0
    unaccounted             :int = 0

@validate_call
def get_rows(session, sql_table, selection, filter_args:dict={}):
    try:
        # preproces selction (map strings)
        the_selection=[]
        for i,k in enumerate(selection):
            sel=k
            if isinstance(k, str):
                if k=='self':
                    sel = sql_table
                elif k in ['primary_key']:
                    sel = get_primary_key(sql_table)
                else:
                    assert hasattr(sql_table, k), f"Requested to return key \'{k}\' from object {sql_table.__name__} but no such attribute {vars(sql_table)}"
                    sel = getattr(sql_table,k)
            the_selection.append(sel)
        query=select(*the_selection)

        # Filters
        for k,v in filter_args.items():
            assert hasattr(sql_table, k), f"Table \'{sql_table.__name__}\' does not have attribute \'{k}\'"
            query=query.where( getattr(sql_table,k)==v )
        
        # Get and return the ids

        ids= session.exec(query).all() 
        # This returns sqlachemy row objects if more then one is selectd
        if len(the_selection)>1:
            ids=[ list(tuple(x)) for x in ids]
        return ids
    except Exception as ex: my_exception(f"Problem in getting ids:", ex)

@validate_call
def get_ids_for_object(session,sql_table):
    selection=get_primary_key(sql_table)
    return get_rows(session, sql_table, [selection], filter_args={'converged':1})

message_tracker_dum=message_tracker
counter_dum=counter
class pop_tracker(myBaseModel):
    session: Session
    messanger :message_tracker_dum = message_tracker()
    counter : counter_dum = counter()
    id_tracker : track_ids=track_ids()
    def get_ids_for_table(self,the_object:SQLModelMetaclass, ids:List[ str|int ]|str='all'):
        """ Get all the ids available (possible filtered) usually just return all """
        self.messanger.start_timing()
        found_ids=get_ids_for_table(self.session,the_object, ids) 
        if isinstance(ids, list):
            self.id_tracker.prerequisites_not_met += [ x for x in  ids if x not in found_ids ]
        self.messanger.stop_timing(f"Filter ids for {the_object.__name__}")

        messanges=[]
        for k,v in self.id_tracker.model_dump().items():
            if len(v)>0:
                messanges+= [ f"{k}: {len(v)}" ]
        if len(messanges)>0:
            self.messanger.add_message( f"Problems in findings ids for {the_object.__name__}: " + '; '.join(messanges) )
        return found_ids
    def build_tree(self, the_objects:List[SQLModelMetaclass]=Field(min_length=2)):
        """ Builds tree for object"""
        tree=get_connections(self.session,the_objects)
        paths={}
        for c in the_objects[1:]:
            if not c.__name__ in tree.keys(): raise Exception(f"Could not map object {c.__name__} from object {the_objects[0].__name__}") 
            paths.update( { c.__name__,tree[c.__name__] })
        from util.sql_util import get_mapper
        mapper=dict([ (name, get_mapper(self.session,path))  for name, path in paths.items() ])
        return mapper