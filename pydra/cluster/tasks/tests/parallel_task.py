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

from pydra.cluster.tasks.parallel_task import ParallelTask
from pydra.cluster.tasks.datasource.slicer import IterSlicer

class ParallelTaskStandaloneTest(unittest.TestCase):
    """
    Test `ParallelTask` functionality without actually running the Twisted
    reactor.
    """

    def setUp(self):
        class pt(ParallelTask):
            datasource = IterSlicer, range(10)
        self.pt = pt()

    def test_trivial(self):
        pass

    def test_get_work_units(self):
        s = set()
        for i, work_unit in enumerate(self.pt.get_work_units()):
            data, index = work_unit
            s.add(index)
            self.assertEqual(data, i)
        self.assertEqual(s, set(self.pt._data_in_progress.keys()))

class ParallelTask_Test(unittest.TestCase):
    """
    Tests for verify functionality of ParllelTask class
    """

    def setUp(self):
        self.parallel_task = TestParallelTask()
        self.worker = WorkerProxy()
        self.parallel_task.parent = self.worker


    def test_key_generation_paralleltask(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask'
        key = self.parallel_task.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


    def test_key_generation_paralleltask_child(self):
        """
        Verifies that the task key used to look up the task is generated correctly
        """
        expected = 'TestParallelTask.TestTask'
        key = self.parallel_task.subtask.get_key()
        self.assertEqual(key, expected, 'Generated key [%s] does not match the expected key [%s]' % (key, expected) )


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


    def test_get_worker_paralleltask(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')


    def test_get_worker_paralleltask_child(self):
        """
        Verifies that the worker can be retrieved
        """
        returned = self.parallel_task.subtask.get_worker()
        self.assert_(returned, 'no worker was returned')
        self.assertEqual(returned, self.worker, 'worker retrieved was not the expected worker')
