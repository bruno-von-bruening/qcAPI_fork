

from . import *
from .wrapper import process_argv, add_client_args, process_client_args, add_property_arg, get_property_args


from server_external.probe_server import probe_server
import argparse

@process_argv
@val_call
def main(argv):
    parser = argparse.ArgumentParser(description="Populate a qcAPI database with jobs")

    parser=add_client_args(parser)
    parser=add_property_arg(parser)

    add=parser.add_argument
    add(
        "--refresh", "-r", type=float, default=1.0, help="refresh rate in seconds"
    )
    add(
        "--worker_delay","-d",
        type=float,
        default=10.,
        help="delay for recent worker check in minutes",
    )
    add(
        "--b1", type=float, default=0.9, help="exponential moving average parameter"
    )
    add(
        "--method", type=str, default='wfn', help='which method to display'
    )

    args = parser.parse_args(argv)
    address, config_file = process_client_args(args)
    property=get_property_args(args)
    delay = args.worker_delay * 60
    property = args.property
    refresh=args.refresh
    method=args.method
    probe_server(address, delay, refresh, property, method)


if __name__ == "__main__":
    main()


# NOTES:
# - use websocket for real-time updates
# - in client, put psi4 in a multiprocessing process and in the main thread periodically check back with the server to see if the job was already compleated by another worker (and to be marked as active)
