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
from twisted.cred import checkers, portal
from twisted.cred.checkers import FilePasswordDB
from twisted.python import failure
from zope.interface import implements
import base64
import os


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


