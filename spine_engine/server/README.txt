How to Execute Projects on Spine Engine Server
==============================================

Setting up Spine Engine Server
------------------------------

1. Make a new environment for spine engine server

- Make new anaconda environment & activate
- Clone and checkout spine-engine. Branch **server**.
- cd to spine-engine repo root, run `pip install -e .`
- Clone and checkout spine-items. Branch **server**.
- cd to spine-items repo root, run `pip install --no-deps -e .`

2. Create security credentials (optional)

- cd to <repo_root>/spine_engine/server/
- Run `python certificate_creator.py`

This creates the security certificates into <repo_root>/spine_engine/server/certs/ directory.

3. Configure allowed endpoints (if security credentials are used)

- Make file <repo_root>/spine_engine/server/connectivity/certs/allowEndpoints.txt
- Add IP addresses of the remote end points to the file

4. Install IPython kernel spec (python3) to enable Jupyter Console execution of Python Tools

- Run `python -m pip install ipykernel` to install and register an IPython kernel (python3) with Jupyter

5. Install Julia 1.8

- Download from https://julialang.org/downloads/ or use `apt-get install julia` on Ubuntu

6. Install IJulia kernel spec (julia-1.8) to enable Jupyter Console execution of Julia tools

- Open Julia REPL and press `]` to enter pkg mode. Enter `add IJulia`
- This installs `julia-1.8` kernel spec to ~/.local/share/jupyter/kernels on Ubuntu or to %APPDATA%\jupyter\kernels

7. Start Spine Engine Server

In directory, <repo_root>/spine_engine/server/

Without security, run:
`python start_server.py tcp 50001`

With Stonehouse security, run:
`python start_server.py tcp 50001 StoneHouse <repo_root>/spine-engine/server/connectivity/certs`

- Arguments: Transport protocol, port, security model (StoneHouse/None), location of the
  security folder.

Note that on the client side, port range has been restricted to 49152-65535.

Setting up Spine Toolbox (client)
---------------------------------

1. Set up an environment
   - Make a new Anaconda environment & activate
   - Clone & checkout spinetoolbox from git. Branch **server**.
   - cd to spinetoolbox repo root, run `pip install -r requirements.txt`

2. Copy security credentials from the server to some directory. Server's secret key does not need to be copied.

3. Start Spine Toolbox

4. Create a project and add some project items.

5. Open the **Engine** page in File->Settings
   - Enable remote execution from the checkbox (Enabled)
   - Set up the remote server settings: host, port, security model, and security folder
   - Click Ok, to close and save the new Settings

6. Click Play to execute the project.
