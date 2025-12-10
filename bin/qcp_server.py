#!/usr/bin/env python

import sys, os, importlib, argparse

from qcpAPI.bin.run_server         import main as main_run
from qcpAPI.bin.populate           import main as main_pop
from qcpAPI.bin.spawn_workers      import main as main_spawn
from qcpAPI.bin.client             import main as main_client
from qcpAPI.bin.server_operations  import main as main_operations 
from qcpAPI.bin.probe_server       import main as main_probe_server
from qcpAPI.bin.restart_server     import main as main_restart



class command_options():
    RUN='RUN'
    POPULATE='POPULATE'
    SPAWN_WORKERS='SPAWN_WORKERS'
    CLIENT='CLIENT'
    OPERATIONS='OPERATIONS'
    PROBE_SERVER='PROBE_SERVER'
    RESTART='RESTART'
    __mapper__=dict(
        RUN=[],
        POPULATE=[],
        SPAWN_WORKERS=[],
        CLIENT=[],
        OPERATIONS=[],
        PROBE_SERVER=[],
        RESTART=[],
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
                    command_options.OPERATIONS, command_options.CLIENT, command_options.PROBE_SERVER,
                    command_options.RESTART,
]

func_mapper=dict(
    RUN           =main_run   ,
    POPULATE      =main_pop   ,
    SPAWN_WORKERS =main_spawn ,
    CLIENT        =main_client,
    OPERATIONS    =main_operations,
    PROBE_SERVER  =main_probe_server,
    RESTART       =main_restart,
)

help_texts={
    command_options.RUN           :"Start the QCP API server",
    command_options.POPULATE      :"Populate the database with tables",
    command_options.SPAWN_WORKERS :"Spawn multiple workers for processing outstanding jobs",
    command_options.CLIENT        :"Single worker",
    command_options.OPERATIONS    :"Manipulate data base (e.g. delete records)",
    command_options.PROBE_SERVER  :"Show completition progress of running jobs",
    command_options.RESTART       :"Send restart command to server (convenience for incorporating updated conda env)",
}


def usage():
    string=f"Usage: {os.path.basename(__file__)} <{','.join(available_commands)}> [args...]"
    info_strings=[]
    for k in available_commands:
        info_strings+=[ (k,help_texts.get(k,'No description available')) ]
    max_key_len=max( len(k) for k in available_commands)
    string+='\nINFO about available commands purpose:\n'+'\n'.join([ f" -  {k:{max_key_len}} : {v}" for k,v in info_strings])
    print(string)

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
