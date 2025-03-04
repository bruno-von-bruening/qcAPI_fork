# qcAPI

Distribute quantum chemistry calculations over different machines following a simple client/server model (REST API).

**Warning**: This is a work in progress and no security measures for the API have been implemented yet.

## Installation

Install dependencies via the provided conda environment:
```bash
  conda env create -f qcAPI_env.yml
  conda activate qcAPI
```
We will need many subpackages to run this script (density_operations, ):
```bash
  git clone "<path_to_script>"
  conda env create -f "<tag>_env.yaml"
  conda activate "<env>"

  # Manage paths and compile f90
  python install/setup.py 

  # Install pip
  python -m pip install --upgrade build  
  python -m build; python -m pip install .
```

## Usage example

Start the server:
```bash
  fastapi run server.py --port 8000
```
The server will create a SQLITE database which name is given in the `config.yaml` file and distribute calculations to clients that connect to it.

In another terminal, populate the database with the example conformers from the provided `test_sample.pkl` file:
```bash
  python populate_db.py test_sample.pkl --address 127.0.0.1:8000 --method hf --basis sto-3g
```
This will create entries in the database for each conformer with the specified method and basis set and list them as *pending* so they can be distributed to clients.

Start a client to process the pending calculations:
```bash
  python client.py --address 127.0.0.1:8000 --num_threads 4
```
This will start a psi4 client with 4 threads that will process the pending calculations in the database.
You can run this command multiple times to start multiple clients, and on different machines to distribute the calculations (provided the server is accessible from the client).

In another terminal, you can check the progress with:
```bash
  python probe_server.py --address 127.0.0.1:8000 
```

## Accessing server from a different machine
In case the server runs on a machine that requires ssh access one can bind the remote server under a port at the local host:
```bash
ssh -L <port_local_host>:<remote_ip>:<port_remote_host> <username>@<remote_ip>
```
Assuming port 8080 has been bound at local machine we can reach the server via "http://localhost:8080"
