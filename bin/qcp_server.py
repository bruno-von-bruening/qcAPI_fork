#!/usr/bin/env python

import sys, os, importlib

from qcpAPI.bin.run_server         import main as main_run
from qcpAPI.bin.populate_server    import main as main_pop
from qcpAPI.bin.spawn_workers      import main as main_spawn
from qcpAPI.bin.client             import main as main_client
from qcpAPI.bin.server_operations  import main as main_operations 


class command_options():
    RUN='RUN'
    POPULATE='POPULATE'
    SPAWN_WORKERS='SPAWN_WORKERS'
    CLIENT='CLIENT'
    OPERATIONS='OPERATIONS'
    __mapper__=dict(
        RUN=[],
        POPULATE=[],
        SPAWN_WORKERS=[],
        CLIENT=[],
        OPERATIONS=[],
    )
    @classmethod
    def unique_tag(self,input):
        found=[]
        def trim(z): return z.upper().strip()
        def match(x,y):
            return trim(x)==trim(y)
        for k,v in self.__mapper__.items():
            if any( match(input, x) for x in v+[k]): found+=[k]
        assert len(found)==1
        return found[0]

available_commands=[ command_options.RUN, command_options.POPULATE , command_options.SPAWN_WORKERS,
                    command_options.OPERATIONS]

func_mapper=dict(
    RUN           =main_run   ,
    POPULATE      =main_pop   ,
    SPAWN_WORKERS =main_spawn ,
    CLIENT        =main_client,
    OPERATIONS    =main_operations,
)


def usage():
    print(f"Usage: {os.path.basename(__file__)} <{','.join(available_commands)}> [args...]")

def main():
    # Expect first argument as the subcommand
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    subcommand = sys.argv[1]
    sub_args = sys.argv[2:]


    try:
        tag=command_options.unique_tag(subcommand)
    except Exception as ex:
        tag=subcommand

    if tag in func_mapper.keys(): func=func_mapper[tag]
    elif tag in available_commands:
        print(f"Not implemented yet: {tag}")
    else:
        print(f"Do not know subcommand: \'{tag.upper()}\'")
        usage()
        sys.exit(1)
        
    func(sub_args)

    
if __name__=='__main__':
    main()
