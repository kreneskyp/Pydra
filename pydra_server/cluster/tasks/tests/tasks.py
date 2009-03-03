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
    tasks_suite.addTest(Task_Test('test_key_generation_task'))
    tasks_suite.addTest(Task_Test('test_key_generation_containertask'))
    tasks_suite.addTest(Task_Test('test_key_generation_paralleltask'))
    tasks_suite.addTest(Task_Test('test_key_generation_containertask_child'))
    tasks_suite.addTest(Task_Test('test_key_generation_paralleltask_child'))

    return tasks_suite


class Task_Test(unittest.TestCase):
    """
    Tests for verify functionality of Task class
    """
    def setUp(self):
        self.task = TestTask()
        self.container_task = TestContainerTask()
        self.parallel_task = TestParallelTask()

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

