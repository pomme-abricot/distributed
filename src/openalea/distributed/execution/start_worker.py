from openalea.distributed.zmq.worker import start_workers
import os
from os.path import expanduser
import json

home = expanduser("~")
tpath = os.path.join(home, "data/mydatalocal")  

with open('tmp.json', 'r') as f:
    kwargs = json.load(f)

start_workers(cache_path=tpath, **kwargs)