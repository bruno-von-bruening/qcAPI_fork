#!/usr/bin/env python

import argparse

from server_executions.probe_server import probe_server

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate a qcAPI database with jobs")
    parser.add_argument(
        "--address", type=str, default="0.0.0.0:8000", help="URL:PORT of the qcAPI server",
    )
    parser.add_argument(
        "--refresh", "-r", type=float, default=1.0, help="refresh rate in seconds"
    )
    parser.add_argument(
        "--worker_delay","-d",
        type=float,
        default=10.,
        help="delay for recent worker check in minutes",
    )
    parser.add_argument(
        "--b1", type=float, default=0.9, help="exponential moving average parameter"    
    )
    parser.add_argument(
        "--property", '-p', type=str, default='wfn', help='which property to display'
        )
    parser.add_argument(
        "--method", type=str, default='wfn', help='which method to display'
        )

    args = parser.parse_args()
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    delay = args.worker_delay * 60
    property = args.property
    refresh=args.refresh
    address = f"http://{url}:{port}"
    method=args.method
    probe_server(address, delay, refresh, property, method)


# NOTES:
# - use websocket for real-time updates
# - in client, put psi4 in a multiprocessing process and in the main thread periodically check back with the server to see if the job was already compleated by another worker (and to be marked as active)
