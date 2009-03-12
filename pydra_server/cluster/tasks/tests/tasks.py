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

import unittest
from pydra_server.cluster.tasks.tasks import *
from pydra_server.cluster.worker import Worker
from pydra_server.task_cache.demo_task import *

from twisted.trial import unittest as twisted_unittest
from twisted.internet import reactor, defer
from threading import Lock, Condition

def suite():
    """
    Build a test suite from all the test suites in this module
    """
    tasks_suite = unittest.TestSuite()

    # key generation
    tasks_suite.addTest(Task_Test('test_key_generation_task'))
    tasks_suite.addTest(Task_Test('test_key_generation_containertask'))
    tasks_suite.addTest(Task_Test('test_key_generation_paralleltask'))
    tasks_suite.addTest(Task_Test('test_key_generation_containertask_child'))
    tasks_suite.addTest(Task_Test('test_key_generation_paralleltask_child'))

    # subtask lookup
    tasks_suite.addTest(Task_Test('test_get_subtask_task'))
    tasks_suite.addTest(Task_Test('test_get_subtask_containertask'))
    tasks_suite.addTest(Task_Test('test_get_subtask_paralleltask'))
    tasks_suite.addTest(Task_Test('test_get_subtask_containertask_child'))
    tasks_suite.addTest(Task_Test('test_get_subtask_paralleltask_child'))

    # worker lookup
    tasks_suite.addTest(Task_Test('test_get_worker_task'))
    tasks_suite.addTest(Task_Test('test_get_worker_containertask'))
    tasks_suite.addTest(Task_Test('test_get_worker_paralleltask'))
    tasks_suite.addTest(Task_Test('test_get_worker_containertask_child'))
    tasks_suite.addTest(Task_Test('test_get_worker_paralleltask_child'))

    #task functions
    tasks_suite.addTest(Task_TwistedTest('test_start'))

    return tasks_suite


class WorkerProxy():
    """
    Class for proxying worker functions
    """
    worker_key = "WorkerProxy"

    def get_worker(self):
        return self


class StartupAndWaitTask(Task):
    """
    Task that runs indefinitely.  Used for tests that
    require a task with state STATUS_RUNNING.  This task
    uses a lock so that the testcase can request the lock
    and effectively pause the task at specific places to
    verify its internal state
    """

    def __init__(self):
        self.starting_lock = Condition(Lock())   # used to lock task until _work() is called
        self.is_started_lock = Condition(Lock()) # used to lock task at the beginning of _work()
        self.running_lock = Condition(Lock())    # used to lock running loop
        self.finished_lock = Condition(Lock())   # used to lock until Task.work() is complete
        self.failsafe_delayed = None
        Task.__init__(self)

    def failsafe(self):
        """
        This is a failsafe for the thread locks.  This function will release any
        locks that are held by the task.
        """
        print 'FAILSAFE ENVOKED!'
        if self.starting_lock.locked():
            self.starting_lock.release()

        if self.finished_lock.locked():
            self.finished_lock.release()


    def work(self, args={}, callback=None, callback_args={}):
        """
        extended to add locks at the end of the work
        """
        try:
            self.failsafe_delayed = reactor.callLater(10, self.failsafe)

            #print 'StartupAndWaitTask - work'
            with self.finished_lock:
                #print 'StartupAndWaitTask - work, got finished_lock'
                Task.work(self, args, callback, callback_args)

        finally:
            if self.failsafe_delayed:
                self.failsafe_delayed.cancel()

    def _work(self, **kwargs):
        """
        'Work' until an external object modifies the STOP_FLAG flag
        """
        #_work has been called so we know were started, release 
        # starting_lock this will allow task to proceed
        #print 'StartupAndWaitTask - _work'
        with self.starting_lock:
               self.starting_lock.notify()
        #print 'StartupAndWaitTask - _work: released starting_lock, waiting for is_started_lock'


        # wait here for TestCase to release lock, this allows
        # testcase to check state at this point
        with self.is_started_lock:
            while not self.STOP_FLAG:
                # wait for the running_lock.  This allows
                # TestCase to prevent needless looping
                # until it is ready to stop the task
                with self.running_lock:
                    pass
        #print 'StartupAndWaitTask: work finished'


class Task_TwistedTest(twisted_unittest.TestCase):
    """
    Task Tests that require the twisted framework to test
    """
    timeout = 10

    def setUp(self):
        #reactor.run()
        print reactor.running
        pass

    def tearDown(self):
        #reactor.stop()
        pass

    def failsafe(self, task=None):
        print 'TESTCASE FAILSAFE'
        if task:
            task.is_started_lock.release()
            task.starting_lock.release()
            task.running_lock.release()


    def wait_for_status(self, task, status, condition):
        """
        Waits for a status to change.
        """
        try:
            condition.acquire()
            while not task.status() == status:
                condition.wait(1)

        finally:
            if condition._Condition__lock.locked():
                condition.release()

    def verify_status(self, task=None):
        try:

            self.failsafe_delayed = reactor.callLater(10, self.failsafe, task=task)

            #acquire locks
            task.is_started_lock.acquire()
            task.running_lock.acquire()
            task.start()

            # wait for lock indicating task has started
            self.wait_for_status(task, STATUS_RUNNING, task.starting_lock)
            self.assertEqual(task.status(), STATUS_RUNNING, 'Task started but status is not STATUS_RUNNING')
            task.is_started_lock.release()

            task._stop()
            # don't release running lock till this point.  otherwise
            # the task will just loop indefinitely.
            task.running_lock.release()

            #wait for the task to finish
            with task.finished_lock:
                self.assertEqual(task._status, STATUS_COMPLETE, 'Task stopped by status is not STATUS_COMPLETE')


        finally:
            #release locks
            if task.is_started_lock._Condition__lock.locked():
                task.is_started_lock.release()

            if task.running_lock._Condition__lock.locked():
                task.running_lock.release()

            #try to stop task no matter what
            if task:
                task._stop()

            if self.failsafe_delayed:
                self.failsafe_delayed.cancel()

    def test_start_task(self):
        """
        Tests Task.start()
            verify:
                * that the status starts with STATUS_STOPPED
                * that the work method is deferred to a thread successfully
                * that the status changes to STATUS_RUNNING when its running
                * that the status changes to STATUS_COMPLETED when its finished
        """
        task = StartupAndWaitTask()
        task.parent = WorkerProxy()

        self.assertEqual(task.status(), STATUS_STOPPED, 'Task did not initialize with status STATUS_STOPPED')

        # defer rest of test because this will cause the reactor to start
        return threads.deferToThread(self.verify_status, task=task)


    '''def test_start_subtask(self):
        """
        Tests Task.start()
            verify:
                * that the status starts with STATUS_STOPPED
                * that the work method is deferred to a thread successfully
                * that the status changes to STATUS_RUNNING when its running
                * that the status changes to STATUS_COMPLETED when its finished
        """
        task = ParallelTask()
        task.subtask = StartupAndWaitTask()
        task.parent = WorkerProxy()

        self.assertEqual(task.status(), STATUS_STOPPED, 'Task did not initialize with status STATUS_STOPPED')

        # defer rest of test because this will cause the reactor to start
        d = threads.deferToThread(task.start, subtask_key='ParallelTask.StartupAndWaitTask')
        return d.addCallback(self.verify_status, task=task)
    '''

class Task_Test(unittest.TestCase):
    """
    Tests for verify functionality of Task class
    """
    def setUp(self):
        self.task = TestTask()
        self.container_task = TestContainerTask()
        self.parallel_task = TestParallelTask()

        self.worker = WorkerProxy()
        self.task.parent = self.worker
        self.container_task.parent = self.worker
        self.parallel_task.parent = self.worker

    def tearDown(self):
        pass

    def test_key_generation_task(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestTask'
        key = self.task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_key_generation_containertask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestContainerTask'
        key = self.container_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_key_generation_paralleltask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask'
        key = self.parallel_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_key_generation_containertask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        for i in range(len(self.container_task.subtasks)):
            expected = 'TestContainerTask.%i' % i
            key = self.container_task.subtasks[i].task.get_key()
            self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )

    def test_key_generation_paralleltask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask.TestTask'
        key = self.parallel_task.subtask.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


    def test_get_subtask_task(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestTask'
        expected = self.task
        returned = self.task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.task.get_subtask, key.split('.'))


    def test_get_subtask_containertask(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestContainerTask'
        expected = self.container_task
        returned = self.container_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))


    def test_get_subtask_paralleltask(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestParallelTask'
        expected = self.parallel_task
        returned = self.parallel_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.parallel_task.get_subtask, key.split('.'))


    def test_get_subtask_containertask_child(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        for i in range(len(self.container_task.subtasks)):
            key = 'TestContainerTask.%i' % i
            expected = self.container_task.subtasks[i].task
            returned = self.container_task.get_subtask(key.split('.'))
            self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'TestContainerTask.10'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))

        # Invalid Key (must be integer)
        key = 'TestContainerTask.FakeTaskThatIsntAnInteger'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))


    def test_get_subtask_paralleltask_child(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        key = 'TestParallelTask.TestTask'
        expected = self.parallel_task.subtask
        returned = self.parallel_task.get_subtask(key.split('.'))
        self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'TestParallelTask.FakeTaskThatDoesNotExist'
        self.assertRaises(TaskNotFoundException, self.parallel_task.get_subtask, key.split('.'))


    def test_get_worker_task(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')

    def test_get_worker_containertask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.container_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


    def test_get_worker_paralleltask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


    def test_get_worker_containertask_child(self):
        """
        Verifies that the worker can be retrieved
        """
        for i in range(len(self.container_task.subtasks)):
            returned = self.container_task.subtasks[i].task.get_worker()
            self.assert_(returned, 'no worker was returned')
            self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


    def test_get_worker_paralleltask_child(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.subtask.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')
