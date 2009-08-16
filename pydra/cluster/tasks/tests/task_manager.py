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
import os
import time
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.task_cache.demo_task import TestTask, TestContainerTask, TestParallelTask
from pydra.models import TaskInstance


def suite():
    """
    Build a test suite from all the test suites in this module
    """
    task_manager_suite = unittest.TestSuite()
    task_manager_suite.addTest(TaskManager_Test('test_register'))
    task_manager_suite.addTest(TaskManager_Test('test_deregister'))
    task_manager_suite.addTest(TaskManager_Test('test_autodiscover'))
    task_manager_suite.addTest(TaskManager_Test('test_listtasks'))

    return task_manager_suite



class TaskManager_Test(unittest.TestCase):

    def setUp(self):

        self.tasks = ['TestTask','TestContainerTask','TestParallelTask']
        self.completion = {}

        for task in self.tasks:
            self.completion[task] = None

        for task in self.tasks [:2]:
            #queued tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.save()

            #running tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = time.strftime('%Y-%m-%d %H:%M:%S')
            task_instance.save()

            #finished tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = time.strftime('%Y-%m-%d %H:%M:%S')
            completed_time = time.strftime('%Y-%m-%d %H:%M:%S')
            task_instance.completed = completed_time
            task_instance.save()
            self.completion[task] = completed_time

            #failed tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = time.strftime('%Y-%m-%d %H:%M:%S')
            task_instance.status = -1
            task_instance.save()


    def tearDown(self):
        pass


    def test_register(self):
        """
        Verifies that the register function registers a Task
        """
        task_manager = TaskManager()
        task_manager.register('TestTask', TestTask)

        self.assert_(task_manager.registry.has_key('TestTask'), 'Registry does not contain key for task')


    def test_deregister(self):
        """
        Verifies that the register function registers a Task
        """
        task_manager = TaskManager()
        task_manager.register('TestTask', TestTask)
        self.assert_(task_manager.registry.has_key('TestTask'), 'Registry does not contain key for task')
        task_manager.deregister('TestTask')
        self.assertFalse(task_manager.registry.has_key('TestTask'), 'Registry still contains key for task')


    def test_autodiscover(self):
        """
        Tests the Task autodiscovery method
        """
        task_manager = TaskManager()
        task_manager.autodiscover()

        # check that the demo tasks are present.
        # a user may have added more tasks but its impossible
        # for us to know which are there without duplicating what task manager does
        for task in self.tasks:
            self.assert_(task_manager.registry.has_key(task))


    def test_listtasks(self):
        """
        Tests list tasks function to verify it returns all the tasks that it should
        """
        task_manager = TaskManager()
        task_manager.register('TestTask', TestTask)
        task_manager.register('TestContainerTask', TestContainerTask)
        task_manager.register('TestParallelTask', TestParallelTask)
        tasks = task_manager.list_tasks()

        self.assertEqual(len(tasks), 3, "There should be 3 registered tasks")

        for task in self.tasks:
            self.assert_(tasks.has_key(task), 'Task is missing from list tasks')
            recorded_time = self.completion[task]
            list_time = tasks[task]['last_run']
            list_time = list_time.strftime('%Y-%m-%d %H:%M:%S') if list_time else None
            self.assertEqual(recorded_time, list_time, "Completion times for task don't match: %s != %s" % (recorded_time, list_time))
