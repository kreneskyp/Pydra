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

import unittest
from threading import Event
from twisted.trial import unittest as twisted_unittest
from twisted.internet import threads

from pydra.cluster.tasks.tasks import *
from pydra.task_cache.demo_task import *
from proxies import *

def suite():
    """
    Build a test suite from all the test suites in this module
    """
    tasks_suite = unittest.TestSuite()

    tasks_suite.addTest(Task_Test(''))

    return tasks_suite


class StatusSimulatingTaskProxy():
    """
    Task Proxy for simulating status
    """
    value = 0
    _status = None

    def __init__(self):
        self._status = STATUS_STOPPED

    def status(self):
        return self._status

    def progress(self):
        return self.value


class TaskContainer_Test(twisted_unittest.TestCase):
    """
    Tests for TaskContainer that require trial (twisted) to run
    """

    def setup(self):
        pass

    def test_progress_auto_weighting(self):
        """
        Tests TaskContainer.progress() with auto weighting on all subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.assertEqual(ctask.progress(), 0, 'Both task progresses are zero, container progress should be zero')

        task1.value = 50
        self.assertEqual(ctask.progress(), 25, 'Values are [50,0] with auto weighting, container progress should be 25%')

        task1.value = 100
        self.assertEqual(ctask.progress(), 50, 'Values are [100,0] with auto weighting, container progress should be 50%')

        task2.value = 50
        self.assertEqual(ctask.progress(), 75, 'Values are [100,50] with auto weighting, container progress should be 75%')

        task2.value = 100
        self.assertEqual(ctask.progress(), 100, 'Values are [100,100] with auto weighting, container progress should be 100%')


    def test_progress_with_one_weighted(self):
        """
        Tests TaskContainer.progress() with manual weighting on only 1 subtask
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1, 80)
        ctask.add_task(task2)

        self.assertEqual(ctask.progress(), 0, 'Both task progresses are zero, container progress should be zero')

        task1.value = 50
        self.assertEqual(ctask.progress(), 40, 'Values are [50,0] with manual weighting 80% on task 1, container progress should be 40%')

        task1.value = 100
        self.assertEqual(ctask.progress(), 80, 'Values are [100,0] with manual weighting 80% on task 1, container progress should be 80%')

        task2.value = 50
        self.assertEqual(ctask.progress(), 90, 'Values are [100,50] with manual weighting 80% on task 1, container progress should be 90%')

        task2.value = 100
        self.assertEqual(ctask.progress(), 100, 'Values are [100,100] with manual weighting 80% on task 1, container progress should be 100%')


    def test_progress_with_one_weighted_multiple_auto(self):
        """
        Tests TaskContainer.progress() with manual weighting on only 1 subtask
        and multiple subtasks with automatic rating
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()
        task3 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1, 80)
        ctask.add_task(task2)   #should default to 10% of the overall progress
        ctask.add_task(task3)   #should default to 10% of the overall progress

        self.assertEqual(ctask.progress(), 0, 'Both task progresses are zero, container progress should be zero')

        task1.value = 50
        self.assertEqual(ctask.progress(), 40, 'Values are [50,0,0] with manual weighting 80% on task 1, container progress should be 40%')

        task1.value = 100
        self.assertEqual(ctask.progress(), 80, 'Values are [100,0,0] with manual weighting 80% on task 1, container progress should be 80%')

        task2.value = 50
        self.assertEqual(ctask.progress(), 85, 'Values are [100,50,0] with manual weighting 80% on task 1, container progress should be 85%')

        task2.value = 100
        self.assertEqual(ctask.progress(), 90, 'Values are [100,100,0] with manual weighting 80% on task 1, container progress should be 90%')

        task3.value = 50
        self.assertEqual(ctask.progress(), 95, 'Values are [100,100,50] with manual weighting 80% on task 1, container progress should be 95%')

        task3.value = 100
        self.assertEqual(ctask.progress(), 100, 'Values are [100,100,100] with manual weighting 80% on task 1, container progress should be 100%')


    def test_progress_when_status_is_completed(self):
        """
        Tests TaskContainer.progress when the tasks have STATUS_COMPLETE
        set as their status
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        task1._status = STATUS_COMPLETE
        task2._status = STATUS_COMPLETE

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.assertEqual(ctask.progress(), 100, 'Container task should report 100 because status is STATUS_COMPLETE')

    def verify_status(self, status1, status2, expected, task):
        """
        helper function for verifying containertask's status
        """
        task.subtasks[0].task._status = status1
        task.subtasks[1].task._status = status2

        self.assertEqual(task.status(), expected, 'statuses were [%s, %s] expected status:%s   actual status:%s' % (status1, status2, expected, task.status()))

    def test_status_not_started(self):
        """
        Test status for container task that has no started subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.verify_status(STATUS_STOPPED, STATUS_STOPPED, STATUS_STOPPED, ctask)


    def test_status_any_subtask_running(self):
        """
        Test status for TaskContainer that has any running subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.verify_status(STATUS_RUNNING, STATUS_STOPPED, STATUS_RUNNING, ctask)
        self.verify_status(STATUS_STOPPED, STATUS_RUNNING, STATUS_RUNNING, ctask)

        self.verify_status(STATUS_COMPLETE, STATUS_RUNNING, STATUS_RUNNING, ctask)
        self.verify_status(STATUS_RUNNING, STATUS_COMPLETE, STATUS_RUNNING, ctask)

        self.verify_status(STATUS_PAUSED, STATUS_RUNNING, STATUS_RUNNING, ctask)
        self.verify_status(STATUS_RUNNING, STATUS_PAUSED, STATUS_RUNNING, ctask)


    def test_status_subtask_failed(self):
        """
        Tests for TaskContainer that has any failed subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.verify_status(STATUS_FAILED, STATUS_STOPPED, STATUS_FAILED, ctask)
        self.verify_status(STATUS_STOPPED, STATUS_FAILED, STATUS_FAILED, ctask)

        self.verify_status(STATUS_COMPLETE, STATUS_FAILED, STATUS_FAILED, ctask)
        self.verify_status(STATUS_FAILED, STATUS_COMPLETE, STATUS_FAILED, ctask)

        self.verify_status(STATUS_PAUSED, STATUS_FAILED, STATUS_FAILED, ctask)
        self.verify_status(STATUS_FAILED, STATUS_PAUSED, STATUS_FAILED, ctask)

        self.verify_status(STATUS_RUNNING, STATUS_FAILED, STATUS_RUNNING, ctask)
        self.verify_status(STATUS_FAILED, STATUS_RUNNING, STATUS_RUNNING, ctask)

    def test_status_all_subtask_complete(self):
        """
        Tests for TaskContainer that has all complete subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.verify_status(STATUS_COMPLETE, STATUS_COMPLETE, STATUS_COMPLETE, ctask)


    def test_status_any_subtask_paused(self):
        """
        Tests for TaskContainer that has any paused subtasks
        """
        task1 = StatusSimulatingTaskProxy()
        task2 = StatusSimulatingTaskProxy()

        ctask = TaskContainer('tester')
        ctask.add_task(task1)
        ctask.add_task(task2)

        self.verify_status(STATUS_PAUSED, STATUS_STOPPED, STATUS_PAUSED, ctask)
        self.verify_status(STATUS_STOPPED, STATUS_PAUSED, STATUS_PAUSED, ctask)

        self.verify_status(STATUS_COMPLETE, STATUS_PAUSED, STATUS_PAUSED, ctask)
        self.verify_status(STATUS_PAUSED, STATUS_COMPLETE, STATUS_PAUSED, ctask)

        self.verify_status(STATUS_PAUSED, STATUS_RUNNING, STATUS_RUNNING, ctask)
        self.verify_status(STATUS_RUNNING, STATUS_PAUSED, STATUS_RUNNING, ctask)


    def verify_sequential_work(self, task):
        """
        Helper function for verifying sequential work within a task works properly
        """
        try:
            #start task it should pause in the first subtask
            args = {'data':'THIS_IS_SOME_FAKE_DATA'}
            task.start(args=args)

            for subtask in task.subtasks:
                # wait for event indicating subtask has started
                subtask.task.starting_event.wait(5)
                self.assertEqual(task.status(), STATUS_RUNNING, 'Task started but status is not STATUS_RUNNING')
                self.assertEqual(subtask.task.status(), STATUS_RUNNING, 'Task started but status is not STATUS_RUNNING')
                self.assertEqual(subtask.task.data, args, 'task did not receive data')
                subtask.task._stop()

                # don't release running lock till this point.  otherwise
                # the subtask will just loop indefinitely and may starve
                # other threads that need to execute
                subtask.task.running_event.set()

                #wait for the subtask to finish
                subtask.task.finished_event.wait(5)
                self.assertEqual(subtask.task._status, STATUS_COMPLETE, 'Task stopped by status is not STATUS_COMPLETE')

            #test that container is marked as finished
            self.assertEqual(task._status, STATUS_COMPLETE, 'Task stopped by status is not STATUS_COMPLETE')

        except Exception, e:
            print 'Exception while testing: %s' % e

        finally:
            #release events just in case
            for subtask in task.subtasks:
                subtask.task._stop()
                subtask.task.clear_events()


    def test_sequential_work(self):
        """
        Tests for verifying TaskContainer iterates through subtasks correctly 
        when run in sequential mode
        """

        task1 = StartupAndWaitTask()
        task2 = StartupAndWaitTask()

        ctask = TaskContainer('tester')
        ctask.parent = WorkerProxy()
        ctask.add_task(task1)
        ctask.add_task(task2)
        return threads.deferToThread(self.verify_sequential_work, task=ctask)


class TaskContainer2_Test(unittest.TestCase):
    """
    Tests for verify functionality of Task class
    """
    def setUp(self):
        self.container_task = TestContainerTask()
        self.worker = WorkerProxy()
        self.container_task.parent = self.worker

    def tearDown(self):
        pass


    def test_key_generation_containertask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestContainerTask'
        key = self.container_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


    def test_key_generation_containertask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        for i in range(len(self.container_task.subtasks)):
            expected = 'TestContainerTask.%i.TestTask' % i
            key = self.container_task.subtasks[i].task.get_key()
            self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


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


    def test_get_subtask_containertask_child(self):
        """
        Verifies:
             * that the task key returns the correct task if given the correct key
             * that the task key returns an error if given an incorrect key
        """
        # correct key
        for i in range(len(self.container_task.subtasks)):
            key = 'TestContainerTask.%i.TestTask' % i
            expected = self.container_task.subtasks[i].task
            returned = self.container_task.get_subtask(key.split('.'))
            self.assertEqual(returned, expected, 'Subtask retrieved was not the expected Task')

        # incorrect Key
        key = 'TestContainerTask.10.TestTask'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))
        key = 'TestContainerTask.10'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))

        # Invalid Key (must be integer)
        key = 'TestContainerTask.FakeTaskThatIsntAnInteger'
        self.assertRaises(TaskNotFoundException, self.container_task.get_subtask, key.split('.'))


    def test_get_worker_containertask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.container_task.get_worker()
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
