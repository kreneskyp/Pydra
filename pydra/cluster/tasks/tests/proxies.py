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

from threading import Event
from twisted.internet import reactor
from pydra.cluster.tasks.tasks import Task


class WorkerProxy():
    """
    Class for proxying worker functions
    """
    worker_key = "WorkerProxy"

    def get_worker(self):
        return self

    def get_key(self):
        return None


class StartupAndWaitTask(Task):
    """
    Task that runs indefinitely.  Used for tests that
    require a task with state STATUS_RUNNING.  This task
    uses a lock so that the testcase can request the lock
    and effectively pause the task at specific places to
    verify its internal state
    """

    def __init__(self):
        self.starting_event = Event()   # used to lock task until _work() is called
        self.running_event = Event()    # used to lock running loop
        self.finished_event = Event()   # used to lock until Task.work() is complete
        self.failsafe = None
        self.data = None
        Task.__init__(self)

    def clear_events(self):
        """
        This clears threads waiting on events.  This function will
        call set() on all the events to ensure nothing is left waiting.
        """
        self.starting_event.set()
        self.running_event.set()
        self.finished_event.set()


    def work(self, args={}, callback=None, callback_args={}):
        """
        extended to add locks at the end of the work
        """
        try:
            # set a failsafe to ensure events get cleared
            self.failsafe = reactor.callLater(5, self.clear_events)

            ret = Task.work(self, args, callback, callback_args)
            self.finished_event.set()

        finally:
            if self.failsafe:
                self.failsafe.cancel()

        return ret

    def _work(self, data=None):
        """
        'Work' until an external object modifies the STOP_FLAG flag
        """
        self.data = data
        self.starting_event.set()

        while not self.STOP_FLAG:
            # wait for the running_event.  This  prevents needless looping
            # and still simulates a task that is working
            self.running_event.wait(5)

        return self.data
