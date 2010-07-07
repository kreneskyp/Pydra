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
import os
import unittest

from Crypto.PublicKey import RSA
from twisted.python.randbytes import secureRandom
from django.utils import simplejson
from pydra.cluster.auth.rsa_auth import RSAAvatar, RSAClient, generate_keys, load_crypto

"""
This file contains tests related to the rsa_auth handshaking used
within pydra
"""

KEY_FILE = 'testkey.key'
KEY_SIZE = 512

class RSA_KeyPair_Test(unittest.TestCase):

    def setUp(self):
        #make sure file doesn't exist yet
        self.destroy_key_files()

    def tearDown(self):
        #make sure file isn't left behind
        self.destroy_key_files()

    def create_key_file(self, data):
        """
        Helper function for creating files
        """
        try:
            f = file(KEY_FILE,'w')
            f.write(simplejson.dumps(data))
        finally:
            if f:
                f.close()

    def destroy_key_files(self):
        """
        helper function for cleanup
        """
        if os.path.exists(KEY_FILE):
            os.remove(KEY_FILE)

    def verify_keys(self, pub_key, priv_key):
        """
        helper function for verifying two keys work together
        """
        bytes = KEY_SIZE/16
        bytes = secureRandom(bytes)

        enc = pub_key.encrypt(bytes, None)
        dec = priv_key.decrypt(enc)
        self.assertEqual(bytes, dec, 'Publickey encrypted bytes could not be decrypted by Privatekey')

        enc = priv_key.encrypt(bytes, None)
        dec = priv_key.decrypt(enc)
        self.assertEqual(bytes, dec, 'Privatekey encrypted bytes could not be decrypted by Privatekey')

    def test_generate_keys(self):
        """
        Test generation of keys.

        validate:
            * structure of key
            * key pair functions
        """
        pub, priv = generate_keys(KEY_SIZE)

        #check array lengths
        self.assertEqual(len(pub), 2, 'Publickey array incorrect size')
        self.assertEqual(len(priv), 5, 'Privatekey array incorrect size')

        pub_key = RSA.construct(pub)
        priv_key = RSA.construct(priv)

        self.verify_keys(pub_key, priv_key)

    def test_load_crypto_public_key(self):
        """
        verifies that a public key can be loaded
        """
        self.destroy_key_files()
        try:
            # create keys and save the public key
            pub, priv = generate_keys(KEY_SIZE)
            self.create_key_file(pub)

            #get the public key data back
            pub_key = load_crypto(KEY_FILE, False, both=False)
            priv_key = RSA.construct(priv)

            self.verify_keys(pub_key, priv_key)
        finally:
            self.destroy_key_files()

    def test_load_crypto_key_pair(self):
        """
        verifies that a key pair can be loaded
        """
        self.destroy_key_files()
        try:
            # create keys and save the public key
            pub, priv = generate_keys(KEY_SIZE)
            self.create_key_file(priv)

            #get the public key data back
            pub, priv_key = load_crypto(KEY_FILE, False)
            pub_key = RSA.construct(pub)

            self.verify_keys(pub_key, priv_key)
        finally:
            self.destroy_key_files()


    def test_load_crypto_key_pair_no_key(self):
        """
        verifies that a key will not be created or loaded when there is no
        Key file and create flag = False
        """
        self.destroy_key_files()
        try:
            priv = load_crypto(KEY_FILE, False, both=False)

            self.assertFalse(priv, 'Private key should not have been created')

        finally:
            self.destroy_key_files()


    def test_load_crypto_key_pair_no_key_create_key(self):
        """
        Verifies keypair will be generated and returned when there is
        no key and create flag = True
        """
        self.destroy_key_files()
        try:
            pub, priv_key = load_crypto(KEY_FILE, key_size=KEY_SIZE)

            self.assert_(pub, 'No public key was returned from load_crypto')
            self.assert_(priv_key, 'No private key was returned from load_crypto')
            self.assertEqual(len(pub), 2, 'Public key should have 2 integers')

            pub_key = RSA.construct(pub)

            self.verify_keys(pub_key, priv_key)

        finally:
            self.destroy_key_files()


class RSA_RSAAvatar_Test(unittest.TestCase):
    """
    Tests for RSAAvatar, this is the server side of the RSA keypair handshake
    """
    def setUp(self):
        pub, priv = generate_keys(KEY_SIZE)
        self.pub_key = RSA.construct(pub)
        self.priv_key = RSA.construct(priv)
        self.callback_avatar = None

    def verify_challenge(self, enc, challenge):
        """
        helper function for verifying challenges
        """
        response = self.create_response(enc)
        self.assertEqual(response, challenge, 'challenge did not match')

    def create_response(self, enc):
        """
        helper function for creating the response
        """
        dec = self.priv_key.decrypt(enc)
        reenc = self.priv_key.encrypt(dec, None)
        hashed = hashlib.sha512(reenc[0]).hexdigest()
        return hashed

    def callback(self, avatar):
        """
        Dummy callback used for testing callback feature.  Sets
        a value in this testcase that can be verified by the test method
        """
        self.callback_avatar=avatar


    def test_challenge_no_key(self):
        """
        Tests challenge function if there is no key for the client.
        """
        avatar = RSAAvatar(self.priv_key, None, None)
        result = avatar.perspective_auth_challenge()
        self.assertEquals(result, -1, 'No public key for client, challenge result should be error (-1)')


    def test_challenge_no_key_first_use(self):
        """
        Tests challenge function when there is no key, but the first_use flag is set.
        """
        avatar = RSAAvatar(self.priv_key, None, None, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()

        # challenge should be None, no_key_first_use is a flag to allow keyless access the first
        # time authenticating, which happens prior to key exchange
        self.assertFalse(challenge, avatar.challenge)

    def test_challenge(self):
        """
        Test a normal challenge where both keys are present
        """
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()

        self.verify_challenge(challenge, avatar.challenge)

    def test_response(self):
        """
        Test the response function given the correct response
        """
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()
        response = self.create_response(challenge)
        result = avatar.perspective_auth_response(response)
        self.assertFalse(result, 'auth_response should return None if handshake is successful')
        self.assert_(avatar.authenticated, 'avatar.authenticated flag should be True if auth_response succeeds')

    def test_bad_response(self):
        """
        Test the response function when given an incorrect response
        """
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()
        #create response that can't be string because its longer than the hash
        response = secureRandom(600)
        result = avatar.perspective_auth_response(response)
        self.assertEqual(result, -1, 'auth_response should return error (-1) when given bad response')
        self.assertFalse(avatar.authenticated, 'avatar.authenticated flag should be False if auth_response fails')

    def test_response_first_use(self):
        """
        Test the response function when first_use_flag is set
        """
        avatar = RSAAvatar(self.priv_key, None, None, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()
        result = avatar.perspective_auth_response(None)
        self.assertFalse(result, 'auth_response should return None if handshake is successful')
        self.assert_(avatar.authenticated, 'avatar.authenticated flag should be True if auth_response succeeds')

    def test_response_before_challenge(self):
        """
        Test sending a response before a challenge has been created
        """
        avatar = RSAAvatar(self.priv_key, None, None, key_size=KEY_SIZE)
        result = avatar.perspective_auth_response(None)
        self.assertEqual(result, 0, 'auth_response should return error (0) when called before auth_challenge')


    def test_success_callback(self):
        """
        Test the callback after a successful auth
        """
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, authenticated_callback=self.callback, key_size=KEY_SIZE)
        challenge = avatar.perspective_auth_challenge()
        response = self.create_response(challenge)
        result = avatar.perspective_auth_response(response)

        self.assert_(self.callback_avatar, 'Callback was not called after success')
        self.assertEqual(self.callback_avatar, avatar, 'Callback was not called after success')




from twisted.internet import defer
class RemoteProxy():
    """
    Class that pretends to be a remote reference.  Used for testing components
    without having to actually start the reactor and make connections.
    """
    def __init__(self):
        self.func = None
        self.args = None
        self.kwargs = None

    def callRemote(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

        deferred = defer.Deferred()
        return deferred


class RSAClient_Test(unittest.TestCase):
    """
    Tests for RSAClient this is the client side of the RSA handshake
    """
    def setUp(self):
        pub, priv = generate_keys(KEY_SIZE)
        self.pub_key = RSA.construct(pub)
        self.priv_key = RSA.construct(priv)

    def test_auth(self):
        """
        Tests the auth function
        """
        client = RSAClient(self.priv_key)
        remote = RemoteProxy()
        client.auth(remote, server_key=self.pub_key)

        self.assertEqual(remote.func, 'auth_challenge', 'Calling auth should trigger auth_challenge call on server')


    def test_auth_challenge(self):
        """
        Tests a normal challenge string
        """
        client = RSAClient(self.priv_key)
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, key_size=KEY_SIZE)
        remote = RemoteProxy()

        challenge = avatar.perspective_auth_challenge()
        client.auth_challenge(challenge, remote, self.pub_key)

        #verify that auth_response got called
        self.assertEqual(remote.func, 'auth_response', 'Calling auth_challenge should trigger auth_response call on server')

        #verify the correct response was sent
        self.assertEqual(remote.kwargs['response'], avatar.challenge, 'Response did not match the expected response')


    def test_auth_challenge_no_server_key(self):
        """
        Tests auth_challenge when server key is None.
        """
        client = RSAClient(self.priv_key)
        avatar = RSAAvatar(self.priv_key, None, self.pub_key, key_size=KEY_SIZE)
        remote = RemoteProxy()

        challenge = avatar.perspective_auth_challenge()
        client.auth_challenge(challenge, remote, None)

        #verify that auth_response got called
        self.assertEqual(remote.func, 'auth_response', 'Calling auth_challenge should trigger auth_response call on server')

        #verify the correct response was sent
        self.assertFalse(remote.kwargs['response'], 'Response did not match the expected response')


    def test_auth_challenge_no_challenge(self):
        """
        Tests auth_challenge when the challenge received is None
        """
        client = RSAClient(self.priv_key)
        remote = RemoteProxy()

        challenge = None
        client.auth_challenge(challenge, remote, self.pub_key)

        #verify that auth_response got called
        self.assertEqual(remote.func, 'auth_response', 'Calling auth_challenge should trigger auth_response call on server')

        #verify the correct response was sent
        self.assertFalse(remote.kwargs['response'], 'Response did not match the expected response')


    def auth_result_success(self):
        """
        Tests responding to a successful auth (no callback)
        """
        #nothing to verify
        pass

    def auth_result_failure(self):
        """
        Tests responding to a failed auth
        """
        #nothing to verify
        pass

    def auth_result_response_before_challenge(self):
        """
        Tests responding to a failure due to response before challenge error
        """
        client = RSAClient(self.priv_key)
        remote = RemoteProxy()

        client.auth_result(0, remote)

        #verify that auth_response got called
        self.assertEqual(remote.func, 'auth_challenge', 'Calling auth_response before auth_challegne should trigger auth_response call on server')

if __name__ == "__main__":
    unittest.main()
