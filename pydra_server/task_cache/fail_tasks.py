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


from pydra_server.cluster.tasks.tasks import *
import time

import logging
logger = logging.getLogger('root')


class FailTask(Task):
    """
    Task that always throws an exception
    """

    description = 'I sleep for 5 seconds, then I intentionally throw an exeption'

    def __init__(self, msg='Demo Task'):
        Task.__init__(self, msg)

    def _work(self, **kwargs):
        time.sleep(5)
        logger.debug('FAILING!!')
        #failing intentionally
        i_dont_exist[0/0]
        logger.debug('FAILed?')

    def progress(self):
        return 0


class FailingContainerTask(TaskContainer):
    """
    Extends TaskContainer only so that it can encapsulate the creation of its children
    """
    description = 'A container task that has children which throw exceptions'

    def __init__(self, msg=None):
        TaskContainer.__init__(self, msg)

        # add some children
        self.add_task(FailTask('child 1'))
        self.add_task(FailTask('child 2'))


class FaillingParallelTask(ParallelTask):
    """
    Example class for running tests in parallel
    """
    description = 'A parallel task that has children which throw exceptions'

    _data = None
    _finished = None

    def __init__(self):
        ParallelTask.__init__(self)
        self.subtask = FailTask('subtask')
        #assign data in init otherwise it could be consumed
        self._data = range(10)
        self._finished = []

    def work_unit_complete(self, data, results):
        logger.info('   Adding results:%s' % results)
        self._finished.append(results)

    def work_complete(self):
        logger.info('tabulating results!')
        logger.info(self._finished)
        