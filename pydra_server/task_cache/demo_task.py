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

from tasks import *
import time

"""
Simple Task used for testing

This task increments a counter and then sleeps 5 seconds.  it will repeat this 5 times.
The counter is returned in a list of arguments so that the data can be passed to another task
if this task is used in a sequence of tasks
"""
class TestTask(Task):
    count = 0
    stop = 5
    description = 'A Demo task that counts to 5, taking a nap after each number'

    def __init__(self, msg='Demo Task'):
        Task.__init__(self, msg)

    """
    Does some simple 'work'
    """
    def _work(self, **kwargs):
        self.count=0
    
        if not (kwargs and kwargs.has_key('data')):
            value = 0
        else :
            value = kwargs['data']

        while self.count < self.stop:
            time.sleep(1)
            self.count += 1
            value += 1
            print 'value: %d   progress: %d%%' % (value , self.progress())

        return {'data':value}

    """
    returns progress as a number between 0 and 100
    """
    def progress(self):
        return 100*self.count/self.stop

    """
    Returns the status as a string
    """
    def progressMessage(self):
        return '%d of %d' % (self.count, self.stop)

    """
    Reset the task - set the counter back to zero
    """
    def _reset(self):
        self.count = 0

"""
Top level task for the test

Extends TaskContainer only so that it can encapsulate the creation of its children
"""
class TestContainerTask(TaskContainer):

    description = 'A demo task that illustrates a ContainerTask.  This task runs 3 TestTasks sequentially'

    def __init__(self, msg=None):
        TaskContainer.__init__(self, msg)

        # add some children
        self.addTask(TestTask('child 1'))
        self.addTask(TestTask('child 2'))
        self.addTask(TestTask('child 3'))

"""
Example class for running tests in parallel
"""
class TestParallelTask(ParallelTask):

    description = 'A demo task illustrating a Parallel task.  This task runs 5 TestTasks at the same time'

    _data = None
    _finished = None

    def __init__(self):
        ParallelTask.__init__(self)
        self.subtask = TestTask('subtask')
        #assign data in init otherwise it could be consumed
        self._data = range(10)
        self._finished = []

    def work_unit_complete(self, data, results):
        print '   Adding results:%s' % results
        self._finished.append(results)

    def work_complete(self):
        print 'tabulating results!'
        print self._finished