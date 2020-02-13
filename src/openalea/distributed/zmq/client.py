import zmq
import json
import dill
import multiprocessing

from openalea.distributed.zmq.client_config import BROKER_ADDR


def start(task, name, *args, **kwargs):
    process = multiprocessing.Process(target=task, name=name, args=args, kwargs=kwargs)
    process.daemon = True
    process.start()


def client_task_fragmenteval(ident, frag, path_out):
    """Basic request-reply client using REQ socket."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect(BROKER_ADDR)
    # Send request, get reply
    request = {'fragment':frag, 'output_path':path_out}
    request = dill.dumps(request)
    socket.send(request)
    reply = socket.recv()
    print(reply)


def client_task_bruteval(ident, num_plant, **kwargs):
    """Basic request-reply client using REQ socket."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect(BROKER_ADDR)
    # Send request, get reply
    request = {"num_plant": num_plant}
    request = dill.dumps(request)
    socket.send(request)
    reply = socket.recv()
    print(reply)