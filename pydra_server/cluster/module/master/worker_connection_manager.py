from pydra_server.cluster.module import Module

class NodeConnectionManager(Module):

    _signals = [
        'WORKER_CONNECTED',
        'WORKER_DISCONNECTED',
    ]

