#!/usr/bin/env python

import sys, os, importlib, argparse

from qcpAPI.bin.run_server         import main as main_run
from qcpAPI.bin.populate_server    import main as main_pop
from qcpAPI.bin.spawn_workers      import main as main_spawn
from qcpAPI.bin.client             import main as main_client
from qcpAPI.bin.server_operations  import main as main_operations 
from qcpAPI.bin.probe_server       import main as main_probe_server



class command_options():
    RUN='RUN'
    POPULATE='POPULATE'
    SPAWN_WORKERS='SPAWN_WORKERS'
    CLIENT='CLIENT'
    OPERATIONS='OPERATIONS'
    PROBE_SERVER='PROBE_SERVER'
    __mapper__=dict(
        RUN=[],
        POPULATE=[],
        SPAWN_WORKERS=[],
        CLIENT=[],
        OPERATIONS=[],
        PROBE_SERVER=[],
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
                    command_options.OPERATIONS, command_options.CLIENT, command_options.PROBE_SERVER ]

func_mapper=dict(
    RUN           =main_run   ,
    POPULATE      =main_pop   ,
    SPAWN_WORKERS =main_spawn ,
    CLIENT        =main_client,
    OPERATIONS    =main_operations,
    PROBE_SERVER  =main_probe_server,
)


def usage():
    print(f"Usage: {os.path.basename(__file__)} <{','.join(available_commands)}> [args...]")

def main():
    # Expect first argument as the subcommand
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    #subcommand = sys.argv[1]
    #sub_args = sys.argv[2:]
    argv=sys.argv[1:]

    #try:
    #    tag=command_options.unique_tag(subcommand)
    #except Exception as ex:
    #    tag=subcommand

    #if tag in func_mapper.keys(): func=func_mapper[tag]
    #elif tag in available_commands:
    #    print(f"Not implemented yet: {tag}")
    #else:
    #    print(f"Do not know subcommand: \'{tag.upper()}\'")
    #    usage()
    #    sys.exit(1)
        
    
    

    pre_parser = argparse.ArgumentParser(description=f"Basis arguments for parser (more if lead keyword provided)",
                                         add_help=False)
    pre_parser.add_argument( f"LEAD_KEYWORD", type=str.upper, # argument will be interpreted as uppercase
                            help="Lead keyword to select sub-parser", choices=func_mapper.keys() )
    pre_args, remaining = pre_parser.parse_known_args(argv)

    func=func_mapper[pre_args.LEAD_KEYWORD]
    func(remaining)

    
if __name__=='__main__':
    main()
