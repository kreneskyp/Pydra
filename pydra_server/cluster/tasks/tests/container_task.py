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
from pydra_server.cluster.tasks.tasks import *
from pydra_server.task_cache.demo_task import *

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
    _status = STATUS_RUNNING
    def progress(self):
        return self.value

class ContainerTask_Test(unittest.TestCase):

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