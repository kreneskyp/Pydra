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
from twisted.trial import unittest as twisted_unittest
from twisted.internet import threads
from threading import Lock, Event

from pydra_server.cluster.tasks.tasks import *
from pydra_server.cluster.worker import Worker
from pydra_server.task_cache.demo_task import *
from proxies import *



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


class Task_TwistedTest(twisted_unittest.TestCase):
    """
    Task Tests that require the twisted framework to test
    """
    timeout = 10

    def setUp(self):
        #reactor.run()
        pass

    def tearDown(self):
        #reactor.stop()
        pass


    def verify_status(self, task, parent,  subtask_key=None):
        try:
            parent.start(subtask_key=subtask_key)

            # wait for event indicating task has started
            task.starting_event.wait(5)
            self.assertEqual(task.status(), STATUS_RUNNING, 'Task started but status is not STATUS_RUNNING')

            task._stop()

            # don't release running lock till this point.  otherwise
            # the task will just loop indefinitely and may starve
            # other threads that need to execute
            task.running_event.set()

            #wait for the task to finish
            task.finished_event.wait(5)
            self.assertEqual(task._status, STATUS_COMPLETE, 'Task stopped by status is not STATUS_COMPLETE')

        except Exception, e:
            print 'Exception while testing: %s' % e


        finally:
            #release events just in case
            task._stop()
            task.clear_events()

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
        return threads.deferToThread(self.verify_status, task=task, parent=task)


    def test_start_subtask(self):
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
        return threads.deferToThread(self.verify_status, task=task.subtask, parent=task, subtask_key='ParallelTask.StartupAndWaitTask')


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
