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

import datetime, time
import hashlib

from twisted.spread import pb
from twisted.internet import reactor, defer
from twisted.python.randbytes import secureRandom
from authenticator import AMFAuthenticator

from django.core.paginator import Paginator, InvalidPage, EmptyPage

from pydra_server.models import TaskInstance

import logging
logger = logging.getLogger('root')

def authenticated(fn):
    """
    decorator for marking functions as requiring authentication.

    this decorator will check the users authentication status and determine
    whether or not to call the function.  This requires that the session id
    (user) be passed with the method call.  The session_id arg isn't required
    by the function itself and will be removed from the list of args sent to 
    the real function
    """
    def new(*args):
        interface = args[0]
        user = args[-1]

        try:
            if interface.sessions[user]['auth']:
                # user is authorized - execute original function
                # strip authentication key from the args, its not needed by the
                # interface and could cause errors.
                return [fn(*(args[:-1]))]

        except KeyError:
            pass # no session yet user must go through authentication

        # user requires authorization
        return 0

    return new


class AMFInterface(pb.Root):
    """
    Interface for Controller.  This exposes functions to a controller.
    """
    def __init__(self, master, checker):
        self.master = master
        self.checker = checker
        self.sessions = {}
        self.session_cleanup = reactor.callLater(20, self.__clean_sessions)

        # Load crypto - The interface runs on the same server as the Master so
        # it can use the same key.  Theres no way with the AMF interface to
        # restrict access to localhost connections only.
        self.key_size=4096
        self.priv_key_encrypt = master.priv_key.encrypt


    def auth(self, user, password):
        """
        Authenticate a client session.  Sessions must initially be 
        authenticated using strict security.  After that a session code can be
        used to quickly authenticate.  The session will timeout after a few 
        minutes and require the client to re-authenticate with a new session 
        code.  This model ensures that session codes are never left active for
        long periods of time.
        """
        if not self.sessions.has_key(user):
            # client has not authenticated yet.  Save session
            authenticator = AMFAuthenticator(self.checker)
            expiration = datetime.datetime.now() + datetime.timedelta(0,120)
            self.sessions[user] = {'code':password, 'expire':expiration, 'auth':False, 'challenge':None}

        return True


    def __clean_sessions(self):
        """
        Remove session that have expired.
        """
        sessions = self.sessions
        now = datetime.datetime.now()
        for k,v in sessions.items():
            if v['expire'] <= now:
                del sessions[k]

        self.session_cleanup = reactor.callLater(20, self.__clean_sessions)


    def authenticate(self, _, user):
        """
        Starts the authentication process by generating a challenge string
        """
        # create a random challenge.  The plaintext string must be hashed
        # so that it is safe to be sent over the AMF service.
        challenge = hashlib.sha512(secureRandom(self.key_size/16)).hexdigest()

        # now encode and hash the challenge string so it is not stored 
        # plaintext.  It will be received in this same form so it will be 
        # easier to compare
        challenge_enc = self.priv_key_encrypt(challenge, None)
        challenge_hash = hashlib.sha512(challenge_enc[0]).hexdigest()

        self.sessions[user]['challenge'] = challenge_hash

        return challenge


    def challenge_response(self, _, user, response):
        """
        Verify a response to a challenge.  A matching response allows
        this instance access to other functions that can manipulate the 
        cluster
        """
        challenge = self.sessions[user]['challenge']
        if challenge and challenge == response:
            self.sessions[user]['auth'] = True

        # destroy challenge, each challenge is one use only.
        self.sessions[user]['challenge'] = None

        return self.authenticated


    @authenticated
    def is_alive(self, _):
        """
        Remote function just for determining that Master is responsive
        """
        logger.debug('is alive')
        return 1


    @authenticated
    def node_status(self, _):
        """
        Returns status information about Nodes and Workers in the cluster
        """
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
                        task_instance_id, task_key, args, subtask_key, workunit_key = self.master._workers_working[w_key]
                        worker_status[html_key] = (1,task_key,subtask_key if subtask_key else -1)
                    else:
                        worker_status[html_key] = -1

            else:
                worker_status=-1

            node_status[key] = {'status':node.status(),
                                'workers':worker_status
                            }

        return node_status


    @authenticated
    def list_tasks(self, _):
        """
        Lists all tasks that can be run.
        """
        return self.master.task_manager.list_tasks()


    @authenticated
    def list_queue(self, _):
        """
        lists tasks in the queue
        """
        return self.master._queue


    @authenticated
    def list_running(self, _):
        """
        lists tasks that are running
        """
        return self.master._running


    @authenticated
    def run_task(self, _, task_key, args=None):
        """
        Runs a task.  It it first placed in the queue and the queue manager
        will run it when appropriate.

        Args should be a dictionary of values.  It is acceptable for this to be
        improperly typed data.  ie. Integer given as a String.  This function
        will parse and clean the args using the form class for the Task
        """

        # args coming from the controller need to be parsed by the form. This
        # will give proper typing to the data and allow validation.
        if args:
            task = self.master.available_tasks[task_key]
            form_instance = task.form(args)
            if form_instance.is_valid():
                # repackage properly cleaned data
                args = {}
                for key, val in form_instance.cleaned_data.items():
                    args[key] = val

            else:
                # not valid, report errors.
                return {
                    'task_key':task_key,
                    'errors':form_instance.errors
                }

        task_instance =  self.master.queue_task(task_key, args=args)

        return {
                'task_key':task_key,
                'instance_id':task_instance.id,
                'time':time.mktime(task_instance.queued.timetuple())
               }


    @authenticated
    def task_history(self, _, key, page):

        instances = TaskInstance.objects.filter(task_key=key).order_by('-completed').order_by('-started')
        paginator = Paginator(instances, 10)

         # If page request (9999) is out of range, deliver last page of results.
        try:
            paginated = paginator.page(page)

        except (EmptyPage, InvalidPage):
            page = paginator.num_pages
            paginated = paginator.page(page)

        return {
                'prev':paginated.has_previous(),
                'next':paginated.has_next(),
                'page':page,
                'instances':[instance for instance in paginated.object_list]
               }


    @authenticated
    def task_statuses(self, _):
        """
        Returns the status of all running tasks.  This is a detailed list
        of progress and status messages.
        """
        return self.master.task_statuses()


    @authenticated
    def cancel_task(self, _, task_id):
        """
        Cancels a task.  This function will either dequeue or cancel a task
        depending on whether it is in the queue or already running
        """
        return self.master.cancel_task(int(task_id))
