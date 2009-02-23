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

from twisted.cred import credentials
from twisted.conch.ssh import keys, factory
from twisted.python.randbytes import secureRandom
from twisted.conch.ssh.keys import Key
from twisted.cred import checkers, portal
from twisted.cred.checkers import FilePasswordDB
from twisted.python import failure
from zope.interface import implements
import base64
import os


def generate_keys():
        """
        Generates an RSA key pair used for connecting to a node.
        keys are returned as the list of values required to serialize/deserilize the keys

        Keys can be reconstructed by RSA.construct(list)
        """
        print "[info] Generating RSA keypair"
        from Crypto.PublicKey import RSA
        KEY_LENGTH = 4096
        rsa_key = RSA.generate(KEY_LENGTH, secureRandom)

        data = Key(rsa_key).data()

        pub_l = [data['n'], data['e']]
        pri_l = [data['n'], data['e'], data['d'], data['q'], data['p']]

        return pub_l, pri_l


def load_crypto(path, create=True):
        """
        Loads RSA keys from the specified path, optionally creating
        new keys.  It automatically detects whether it is a keypair
        or just the public key
        """
        import os
        from django.utils import simplejson
        from Crypto.PublicKey import RSA
        if not os.path.exists(path):
            if create:
                #local key does not exist, create and store
                pub, priv = generate_keys()
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

        return None


from twisted.internet import defer
from twisted.python import failure, log
from twisted.cred import error, credentials
class FirstUseChecker(FilePasswordDB):
    """
    Implementation of a checker that allows the first user to login
    to become registered with the checker.  The credentials are
    stored in the file and from that point forward only that user
    is allowed to login.

    While this temporarily leaves the node open, it allows the node
    to require no configuration in most cases.
    """
    credentialInterfaces = (credentials.IUsernamePassword,)
    def requestAvatarId(self, c):
        #if no file or empty file allow access
        if not os.path.exists(self.filename) or not len(list(self._loadCredentials())):
            return defer.succeed(c.username)

        try:
            u, p = self.getUser(c.username)
        except KeyError:
            return defer.fail(error.UnauthorizedLogin())
        else:
            up = credentials.IUsernamePassword(c, None)
            if self.hash:
                if up is not None:
                    h = self.hash(up.username, up.password, p)
                    if h == p:
                        return defer.succeed(u)
                return defer.fail(error.UnauthorizedLogin())
            else:
                return defer.maybeDeferred(c.checkPassword, p
                    ).addCallback(self._cbPasswordMatch, u)

    def _requestAvatarId(self, c):
        # first check to see if there are any users registered
        # if there are new users, add the user and then continue
        # authorizing them as nomral
        print c.__dict__
        if not os.path.exists(self.filename) or not len(list(self._loadCredentials())):
            #hash password if available
            if self.hash:
                password = self.hash(c.username, c.password, None)
            else:
                password = c.password

            login_str = '%s%s%s' % (c.username, self.delimeter, password)
            #create file if needed
            #os.path.exists(self.filename)
            file(self.filename, 'w').write(login_str)

        return FilePasswordDB.requestAvatarId(self, c)


