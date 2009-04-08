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

import datetime

from twisted.spread import pb
from twisted.internet import reactor

from authenticator import AMFAuthenticator

class AMFInterface(pb.Root):
    """
    Interface for Controller.  This exposes functions to a controller.
    """
    def __init__(self, master, checker):
        self.master = master
        self.checker = checker
        self.sessions = {}
        self.session_cleanup = reactor.callLater(20, self.clean_sessions)

    def auth(self, user, password):
        """
        Authenticate a client session.  Sessions must initially be 
        authenticated using strict security.  After that a session code can be
        used to quickly authenticate.  The session will timeout after a few 
        minutes and require the client to re-authenticate with a new session 
        code.  This model ensures that session codes are never left active for
        long periods of time.
        """
        if self.sessions.has_key(user):
            #client has already authenticated, let it pass
            print '[DEBUG] AMFInterface - quick auth'
            return True

        else:
            #client has not authenticated yet.
            authenticator = AMFAuthenticator(self.checker)
            if authenticator.auth(user, password):
                print '[DEBUG] AMFInterface - real auth'
                expiration = datetime.datetime.now() + datetime.timedelta(0,120)
                self.sessions[user] = {'code':password, 'expire':expiration}
                return True

            else:
                return False

    def clean_sessions(self):
        """
        Remove session that have expired.
        """
        sessions = self.sessions
        now = datetime.datetime.now()
        for k,v in sessions.items():
            if v['expire'] <= now:
                del sessions[k]

        self.session_cleanup = reactor.callLater(20, self.clean_sessions)

    def is_alive(self, _):
        print '[debug] is alive'
        return 1

    def node_status(self, _):
        node_status = {}
        worker_list = self.master.workers
        #iterate through all the nodes adding their status
        for key, node in self.master.nodes.items():
            worker_status = {}
            if node.cores:
                #iterate through all the workers adding their status as well
                #also check for a worker whose should be running but is not connected
                for i in range(node.cores):
                    w_key = '%s:%s:%i' % (node.host, node.port, i)
                    html_key = '%s_%i' % (node.id, i)
                    if w_key in self.master._workers_idle:
                        worker_status[html_key] = (1,-1,-1)
                    elif w_key in self.master._workers_working:
                        task, subtask = self.master._workers_working[w_key]
                        worker_status[html_key] = (1,task,subtask)
                    else:
                        worker_status[html_key] = -1

            else:
                worker_status=-1

            node_status[key] = {'status':node.status(),
                                'workers':worker_status
                               }
        return node_status

    def list_tasks(self, _):
        return self.master.task_manager.list_tasks()

    def list_queue(self, _):
        return self.master._queue

    def run_task(self, _, key):
        return self.master.queue_task(key)

    def task_status(self, _):
        return self.master.task_manager.task_status()

    def cancel_task(self, _, task_id):
        return self.master.cancel_task(int(task_id))
