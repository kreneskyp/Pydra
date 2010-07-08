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

import hashlib
import math
import os
import simplejson

from twisted.spread import pb
from twisted.python.randbytes import secureRandom
from twisted.conch.ssh.keys import Key
from twisted.internet import threads
from Crypto.PublicKey import RSA

import logging
logger = logging.getLogger('root')

class RSAAvatar(pb.Avatar):
    """
    Avatar for authenticating with RSA key pairs.

    RSAAvatar implements a challenge and response that verifies that the
    client and server have matching keys.

    Note: This handshake should be built in as a checker, but the
    PerspectiveBroker API does not suuport ISSHKey credentials for
    authorization.
    """

    def __init__(self, server_key, server_pub_key, client_key, authenticated_callback=None, save_key=None, key_size=4096):
        self.server_key = server_key
        self.server_pub_key = server_pub_key
        self.client_key = client_key
        self.authenticated_callback = authenticated_callback
        self.key_size = key_size
        self.save_key = save_key

        self.authenticated = False
        self.challenged = False
        self.challenge = None

    def attached(self, mind):
        """
        Set the Mind used for remote calls.
        """
        self.remote = mind

    def detached(self, mind):
        """
        Forget the Mind.
        """
        self.remote = None


    def perspective_auth_challenge(self):
        """
        Remote method for starting the challenge-response authorization
        handshake.

        Start by creating a random, signed challenge string that is encrypted
        using the client's public key. Only the client with the correct key
        can decrypt it and send it back.

        If the Avatar does not have the public key for the client attempting
        to connect, it will return -1. This allows the client to trigger the
        key exchange and pairing before retrying.
        """
        if not self.client_key:
            return -1

        challenge = secureRandom(self.key_size/16)

        # encode using master's key, only the matching private
        # key will be able to decode this message
        encrypted = self.client_key.encrypt(challenge, None)[0]

        # now encode and hash the challenge string so it is not stored
        # plaintext.  It will be received in this same form so it will be
        # easier to compare
        challenge = self.server_key.encrypt(challenge, None)
        challenge = hashlib.sha512(challenge[0]).hexdigest()

        self.challenge = challenge
        self.challenged = True

        return encrypted


    def perspective_auth_response(self, response):
        """
        Called to verify a response from a challenger.

        If the avatar hasn't been challenged, it will not respond. The avatar
        will only respond to one single challenger before refreshing its
        challenge, to avoid brute-force and replay attacks.
        """

        if not self.challenged:
            return 0

        # challenge  must be verified before the server will allow init to continue
        verified = self.challenge == response
        # reset the challenge after checking it once.  This prevents brute force attempts
        # to determine the correct response
        self.challenge = None
        self.challenged = False
        if not verified:
            #challenge failed, return error code
            logger.error('failed authentication challenge')
            return -1

        logger.info('verified')

        self.authenticated = True
        if self.authenticated_callback:
            self.authenticated_callback(self)


    def perspective_exchange_keys(self, master_pub_key):
        """
        Exchange public keys with the client.

        This allows the client to authenticate in the using the keypair
        handshake.
        """
        logger.info('Exchanging keys with master')

        # reconstruct key array, it was already encoded
        # with json so no need to encode it here
        json_key = ''.join(master_pub_key)

        if self.save_key:
            self.save_key(json_key)

        key = [long(x) for x in simplejson.loads(json_key)]
        self.client_key = RSA.construct(key)

        #send the nodes public key.  serialize it and encrypt it
        #the key must be broken into chunks for it to be signed
        #for ease recompiling it we'll store the chunks as a list
        key_chunks = []
        for i in self.chunks():
            enc = self.client_key.encrypt(i, None)
            key_chunks.append(enc[0])

        return key_chunks


    def chunks(self):
        json_key = simplejson.dumps(self.server_pub_key)
        key_chunks = []
        chunk = 128
        for i in range(int(math.ceil(len(json_key)/(chunk*1.0)))):
            key_chunks.append(json_key[i*chunk:i*chunk+chunk])
        return key_chunks


    def perspective_get_key(self):
        """
        Returns the public key of the avatar.
        """
        return self.chunks()


class RSAClient(object):
    """
    Class encompassing the client's part of the RSA handshake.

    The main work is done in `auth`.
    """

    def __init__(self, client_priv_key, client_pub_key=None, callback=None, errback=None):
        self.callback = callback
        self.errback  = errback

        self.client_pub_key = client_pub_key
        self.client_priv_key = client_priv_key
        self.client_priv_key.decrypt = client_priv_key.decrypt


    def auth(self, remote, save_key=None, server_key=None, **kwargs):
        """
        Performs an authentication handshake with the specified remote avatar.

        If set, the client's callback will be called on success, or the
        errback on failure.
        """
        if server_key:
            logger.debug('Logging into server')
            d = remote.callRemote('auth_challenge')
            d.addCallback(self.auth_challenge, remote, server_key=server_key,  **kwargs)

        else:
            # no key, exchange keys first.
            logger.info('No public key for server, exchanging keys')
            self.exchange_keys(remote, callback=self.auth, server_key=server_key, save_key=save_key, **kwargs)


    def auth_challenge(self, challenge, remote, server_key=None, **kwargs):
        """
        Callback for a request for authorization challenge.

        This callback will decode and respond to the string passed in.

        If there is a challenge from the Node, it must be answered; otherwise,
        the Node will not allow access to any of its functions.  The challenge
        will be encrypted with the master's key.  It should be decrypted, then
        encrypted with the node's key, and hashed.
        """

        if challenge == -1:
            # -1 code indicates that the server does not have the clients key
            # yet.  Its possible that it was deleted but the client has retained
            # the server's key.  This would allow the client to pass the first
            # check
            self.exchange_keys(remote, callback=self.auth, server_key=server_key, **kwargs)
            return

        if challenge and server_key:
            #decrypt challenge
            challenge_str = self.client_priv_key.decrypt(challenge)

            #re-encrypt using servers key and then sha hash it.
            challenge_encode = server_key.encrypt(challenge_str, None)
            challenge_hash = hashlib.sha512(challenge_encode[0]).hexdigest()
        else:
            challenge_hash = ""

        d = remote.callRemote('auth_response', response=challenge_hash)
        d.addCallback(self.auth_result, remote, **kwargs)


    def auth_result(self, result, remote, **kwargs):
        """
        Callback to handle the result of the challenge-response handshake.
        """
        if result == -1:
            #authentication failed
            logger.error('%s - rejected authentication' % remote)
            if self.errback:
                threads.deferToThread(errback)
            return

        if result == 0:
            # init was called before 'info'.  There was no challenge
            # so the node will prevent a connection.  this is a defensive
            # mechanism to ensure a challenge was created before you init
            logger.warning('%s - auth_result called before request, automatic rety' % remote)
            d = remote.callRemote('auth_challenge')
            d.addCallback(self.auth_challenge, **kwargs)
            return

        #successful! begin init'ing the node.
        if self.callback:
            threads.deferToThread(self.callback, **kwargs)


    def exchange_keys(self, remote, callback=None, server_key=None, **kwargs):
        """
        Starts the key exchange process, also known as "pairing".

        Client and Server will both send their keys to each other, and save
        them for later authentication.
        """
        #twisted.bannana doesnt handle large ints very well
        #we'll encode it with json and split it up into chunks
        dumped = simplejson.dumps(self.client_pub_key)
        chunk = 100
        split = [dumped[i*chunk:i*chunk+chunk] for i in range(int(math.ceil(len(dumped)/(chunk*1.0))))]
        pub_key = split

        d = remote.callRemote('exchange_keys', pub_key)
        d.addCallback(self.exchange_keys_receive, callback, server_key, remote=remote, **kwargs)


    def exchange_keys_receive(self, returned_key_raw, callback=None, server_key=None, save_key=None, **kwargs):
        """
        Receives a key in the exchange process.  This is also known as "pairing"
        The keys that are exchanged are saved for authentication.

        The key is only saved if save_key is a function that can save the key
        to a storage medium of somesort
        """
        dec = [self.client_priv_key.decrypt(chunk) for chunk in returned_key_raw]
        json_key = ''.join(dec)
        key = [long(x) for x in simplejson.loads(json_key)]
        rsa_key = RSA.construct(key)

        if save_key:
            logger.debug('Saving public key from server')
            save_key(json_key, **kwargs)

        if callback:
            threads.deferToThread(callback, save_key=save_key, server_key=rsa_key, **kwargs)


def generate_keys(size=4096):
        """
        Generates an RSA key pair used for connecting to a node.

        Keys are returned as the minimum lists of values required to
        reconstruct the keys mathematically; see Wikipedia's RSA article at
        http://en.wikipedia.org/wiki/RSA for a brief explanation of the maths
        involved. n and e are the public key; n, e, d, q, and p are the
        private key. The ordering is chosen to match `RSA.construct`.

        >>> publist, privlist = generate_keys()
        >>> public = RSA.construct(publist)
        >>> private = RSA.construct(privlist)

        :return: (list(n, e), list(n, e, d, q, p))
        """
        logging.info('Generating RSA keypair')
        KEY_LENGTH = size
        rsa_key = RSA.generate(KEY_LENGTH, secureRandom)

        data = Key(rsa_key).data()

        pub_l = [data['n'], data['e']]
        pri_l = [data['n'], data['e'], data['d'], data['q'], data['p']]

        return pub_l, pri_l


def load_crypto(path, create=True, key_size=4096, both=True):
        """
        Loads RSA keys from the specified path, optionally creating new keys.

        This function automatically detects whether it is a pair of public and
        private keys, or just the public key.
        """
        import os
        if not os.path.exists(path):
            if create:
                #local key does not exist, create and store
                pub, priv = generate_keys(size=key_size)
                try:
                    f = file(path,'w')
                    f.write(simplejson.dumps(priv))
                    os.chmod(path, 0400)
                finally:
                    if f:
                        f.close()

                return pub, RSA.construct(priv)

        else:
            import fileinput
            try:
                key_file = fileinput.input(path)
                priv_raw = simplejson.loads(key_file[0])
                key_all = [long(x) for x in priv_raw]

                if len(key_all) > 2:
                    # file contains both keys
                    pub = key_all[:2]
                    return pub, RSA.construct(key_all)

                #file had pub key only
                return RSA.construct(key_all)

            finally:
                if key_file:
                    key_file.close()
        return (None,None) if both else None
