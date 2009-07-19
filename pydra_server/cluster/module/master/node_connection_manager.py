"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""
from __future__ import with_statement
from threading import Lock

from twisted.cred import credentials
from twisted.internet import reactor, defer
from twisted.internet.error import AlreadyCalled
from twisted.spread import pb

from pydra_server.cluster.module import Module
from pydra_server.cluster.amf.interface import authenticated
from pydra_server.cluster.auth.rsa_auth import RSAClient, load_crypto
from pydra_server.models import Node, pydraSettings


import logging
logger = logging.getLogger('root')

class NodeClientFactory(pb.PBClientFactory):
    """
    Subclassing of PBClientFactory to add auto-reconnect via Master's reconnection code.
    This factory is specific to the master acting as a client of a Node.
    """

    node = None

    def __init__(self, node, manager):
        """
        @param node - node this factory is watching
        @param manager - manager that is tracking this node
        """
        self.node = node
        self.connection_manager = manager
        pb.PBClientFactory.__init__(self)

    def clientConnectionLost(self, connector, reason):
        """
        Called when self.node disconnects
        """
        #lock - ensures that this blocks any connection attempts
        with self.connection_manager._lock:
            self.node.ref = None

        self.connection_manager.reconnect_nodes(True);
        pb.PBClientFactory.clientConnectionLost(self, connector, reason)


class NodeConnectionManager(Module):

    _signals = [
        'WORKER_CONNECTED',
        'WORKER_DISCONNECTED',
        'NODE_CONNECTED',
        'NODE_DISCONNECTED',
        'NODE_AUTHENTICATED'
    ]

    _shared = [
        'nodes',
        'known_nodes',
        'worker_checker',
    ]


    def __init__(self, manager):

        #locks
        self._lock = Lock() #general lock, use when multiple shared resources are touched

        self._listeners = {
            'MASTER_INIT':self.connect,
            'NODE_CREATED':self.connect_node,
            'NODE_UPDATED':self.connect_node,
        }

        self._interfaces = [
            authenticated(self.connect)
        ]

        Module.__init__(self, manager)

        #load rsa crypto
        self.pub_key, self.priv_key = load_crypto('./master.key')
        self.rsa_client = RSAClient(self.priv_key, self.pub_key, callback=self.init_node)

        #cluster management
        self.nodes = self.load_nodes()

        #connection management
        self.connecting = True
        self.reconnect_count = 0
        self.attempts = None
        self.reconnect_call_ID = None

        self.host = 'localhost'


    def load_nodes(self):
        """
        Load node configuration from the database
        """
        logger.info('loading nodes')
        nodes = Node.objects.all()
        node_dict = {}
        for node in nodes:
            node_dict[node.id] = node
        logger.info('%i nodes loaded' % len(nodes))
        return node_dict

    
    def connect_node(self, node):
        """
        Callback for node creation and updates.  Connect() cannot be called
        directly because those signals include a parameter
        """
        self.connect()


    def connect(self):
        """
        Make connections to all Nodes that are not connected.  This method is a single control 
        for connecting to nodes.  individual nodes cannot be connected to.  This is to ensure that
        only one attempt at a time is ever made to connect to a node.
        """
        #lock for two reasons:
        #  1) connect() cannot be called more than once at a time
        #  2) if a node fails while connecting the reconnect call will block till 
        #     connections are finished
        with self._lock:
            self.connecting=True

            # make sure the various states are in sync
            for i in Node.objects.all():
                if i.id not in self.nodes:
                    self.nodes[i.id] = i
                if (i.host, i.port) in self.known_nodes:
                    self.known_nodes.discard((i.host, i.port))

            logger.info("Connecting to nodes")
            connections = []
            self.attempts = []
            for id, node in self.nodes.items():
                #only connect to nodes that aren't connected yet
                if not node.ref:
                    factory = NodeClientFactory(node, self)
                    reactor.connectTCP(node.host, node.port, factory)

                    # SSH authentication is not currently supported with perspectiveBroker.
                    # For now we'll perform a key handshake within the info/init handshake that already
                    # occurs.  Prior to the handshake completing access will be limited to info which
                    # is informational only.
                    #
                    # Note: The first time connecting to the node will accept and register whatever
                    # key is passed to it.  This is a small trade of in temporary insecurity to simplify
                    # this can be avoided by manually generating and setting the keys
                    #
                    #credential = credentials.SSHPrivateKey('master', 'RSA', node.pub_key, '', '')
                    credential = credentials.UsernamePassword('master', '1234')

                    deferred = factory.login(credential, client=self)
                    connections.append(deferred)
                    self.attempts.append(node)

            defer.DeferredList(connections, consumeErrors=True).addCallbacks(
                self.nodes_connected, errbackArgs=("Failed to Connect"))

            # Release the connection flag.
            self.connecting=False


    def nodes_connected(self, results):
        """
        Called with the results of all connection attempts.  Store connections and retrieve info from node.
        The node will respond with info including how many workers it has.
        """
        # process each connected node
        failures = False

        for result, node in zip(results, self.attempts):

            #successes
            if result[0]:
                # save reference for remote calls
                node.ref = result[1]
                d = node.ref.callRemote('get_key')
                d.addCallback(self.check_node, node)


            #failures
            else:
                logger.error('node:%s:%s - failed to connect' % (node.host, node.port))
                node.ref = None
                failures = True


        #single call to reconnect for all failures
        if failures:
            self.reconnect_nodes()

        else:
            self.reconnect_count = 0


    def check_node(self, key, node):
        # node.pub_key is set only for paired nodes, make sure we don't attempt
        # to pair with a known pub key
        duplicate = ''.join(key) in [i.pub_key for i in self.nodes.values()]
        if duplicate and not node.pub_key:
            logger.info('deleting %s:%s - duplicate' % (node.host, node.port))
            node.delete()
            return

        # Authenticate with the node
        pub_key = node.load_pub_key()
        self.rsa_client.auth(node.ref, self.receive_key_node, server_key=pub_key, node=node)

        logger.info('node:%s:%s - connected' % (node.host, node.port))


    def reconnect_nodes(self, reset_counter=False):
        """
        Called to signal that a reconnection attempt is needed for one or more nodes.  This is the single control
        for requested reconnection.  This single control is used to ensure at most 
        one request for reconnection is pending.
        """
        #lock - Blocking here ensures that connect() cannot happen while requesting
        #       a reconnect.
        with self._lock:
            #reconnecting flag ensures that connect is only called a single time
            #it's possible that multiple nodes can have problems at the same time
            #reset_counter overrides this
            if not self.connecting or reset_counter:
                self.connecting = True

                #reset the counter, useful when a new failure occurs
                if reset_counter:
                    #cancel existing call if any
                    if self.reconnect_call_ID:
                        try:
                            self.reconnect_call_ID.cancel()

                        # There is a slight chance that this method can be called
                        # and receive the lock, after connect() has been called.
                        # in that case reconnect_call_ID will point to an already called
                        # item.  The error can just be ignored as the locking will ensure
                        # the call we are about to make does not start
                        # until the first one does.
                        except AlreadyCalled:
                            pass

                    self.reconnect_count = 0

                reconnect_delay = 5*pow(2, self.reconnect_count)
                #let increment grow exponentially to 5 minutes
                if self.reconnect_count < 6:
                    self.reconnect_count += 1 
                logger.debug('reconnecting in %i seconds' % reconnect_delay)
                self.reconnect_call_ID = reactor.callLater(reconnect_delay, self.connect)


    def receive_key_node(self, key, node=None, **kwargs):
        """
        Receives the public key from the node
        """
        logger.debug("saving public key from node: %s" % node)
        node.pub_key = key
        node.save()


    def init_node(self, node):
        """
        Start the initialization sequence with the node.  The first
        step is to query it for its information.
        """
        d = node.ref.callRemote('info')
        d.addCallback(self.add_node, node=node)


    def add_node(self, info, node):
        """
        Process Node information.  Most will just be stored for later use.  Info will include
        a list of workers.  The master will then connect to all Workers.
        """

        # save node's information in the database
        node.cores = info['cores']
        node.cpu_speed = info['cpu']
        node.memory = info['memory']
        node.save()

        #node key to be used by node and its workers
        node_key_str = '%s:%s' % (node.host, node.port)

        # add all workers
        for i in range(node.cores):
            worker_key = '%s:%i' % (node_key_str, i)
            self.worker_checker.addUser(worker_key, '1234')


        # we have allowed access for all the workers, tell the node to init
        d = node.ref.callRemote('init', self.host, pydraSettings.port, node_key_str)
        d.addCallback(self.node_ready, node)


    def node_ready(self, result, node):
        """ 
        Called when a call to initialize a Node is successful
        """
        logger.info('node:%s - ready' % node)

