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

from pydra.cluster.tasks import Task, TaskContainer, ParallelTask
from pydra.cluster.tasks.datasource.slicer import IterSlicer
import time

import logging
logger = logging.getLogger('root')
logger.setLevel(logging.DEBUG)

from django import forms
class TestTaskInput(forms.Form):
    """
    Form object used to render an interface that captures input
    for TestTask.
    """
    start = forms.IntegerField(initial='0', help_text='Start counting with this number')
    end   = forms.IntegerField(initial='5', help_text='End counting with this number')


class TestTask(Task):
    """
    Simple Task used for testing

    This task increments a counter and then sleeps 5 seconds.  it will repeat this 5 times.
    The counter is returned in a list of arguments so that the data can be passed to another task
    if this task is used in a sequence of tasks
    """
    count = 0
    stop = 5
    description = 'A Demo task that counts to 5, taking a nap after each number'
    form = TestTaskInput

    def __init__(self, msg='Demo Task', *args, **kwargs):
        Task.__init__(self, msg)

    def work(self, **kwargs):
        """
        Does some simple 'work'
        """
        self.count=0

        try:
            try:
                value = kwargs['start']
            except KeyError:
                value = 0

            try:
                self.stop = kwargs['end'] - value
            except KeyError:
                pass
        except Exception, e:
            self.logger.error('[%s] test task exception' %(self.get_worker().worker_key, e))

        self.logger.info('[%s] counting from %s to %s' % (self.get_worker().worker_key, value, value+self.stop))

        while self.count < self.stop and not self.STOP_FLAG:
            time.sleep(1)
            self.count += 1
            value += 1
            self.logger.info('[%s] value: %d   progress: %d%%' % (self.get_worker().worker_key, value , self.progress()))

        return {'start':value}

    def progress(self):
        """
        returns progress as a number between 0 and 100
        """
        return 100*self.count/self.stop

    def progressMessage(self):
        """
        Returns the status as a string
        """
        return '%d of %d' % (self.count, self.stop)

    def _reset(self):
        """
        Reset the task - set the counter back to zero
        """
        self.count = 0


class TestContainerTask(TaskContainer):
    """
    Top level task for the test

    Extends TaskContainer only so that it can encapsulate the creation of its children
    """
    description = 'A demo task that illustrates a ContainerTask.  This task runs 3 TestTasks sequentially'

    def __init__(self, msg=None):
        TaskContainer.__init__(self, msg)

        # add some children
        self.add_task(TestTask('child 1'))
        self.add_task(TestTask('child 2'))
        self.add_task(TestTask('child 3'))


class TestParallelTask(ParallelTask):
    """
    Example class for running tests in parallel
    """
    description = 'A demo task illustrating a Parallel task.  This task runs 5 TestTasks at the same time'

    def __init__(self):
        ParallelTask.__init__(self)
        self.set_subtask(TestTask, 'subtask')
        self.input = IterSlicer(range(10))
        self._data = range(10)
        self._finished = []

        logger.setLevel(logging.DEBUG)

    def work_unit_complete(self, data, results):
        logger.info('[%s]   Adding results:%s' % (self.get_worker().worker_key, results))
        self._finished.append(results)

    def work_complete(self):
        logger.info('[%s] tabulating results!' % (self.get_worker().worker_key))
        logger.info(self._finished)
        return self._finished
