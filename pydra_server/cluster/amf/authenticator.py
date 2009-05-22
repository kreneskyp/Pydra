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
import time

import logging
logger = logging.getLogger('root')

class AMFAuthenticator(object):
    """
    Class used to authenticate the request.  This is a hack of a class but
    unfortunately required because there does not appear to be a better way
    to block until a deferred completes
    """
    def __init__(self, checker):
        self.result = False
        self.checker = checker

    def auth_success(self, result):
        self.result = True
        self.auth = True

    def auth_failure(self, result):
        logger.error('Unauthorized attempt to use service')
        self.result = True
        self.auth = False

    def auth(self, user, password):
        from twisted.cred.credentials import UsernamePassword
        credentials = UsernamePassword(user, password)
        avatarId = self.checker.requestAvatarId(credentials)
        avatarId.addCallback(self.auth_success)
        avatarId.addErrback(self.auth_failure)

        # block for 5 seconds or until a result happens
        # in most cases a result should happen very quickly
        for i in range(25):
            if self.result:
                break
            time.sleep(.2)

        return self.auth
