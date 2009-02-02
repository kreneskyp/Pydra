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
    _data = range(5)
    _finished = []

    def __init__(self):
        ParallelTask.__init__(self)
        self.subtask = TestTask('subtask')

    def work_unit_complete(self, data, results):
        print '   Adding results:%s' % results
        self._finished.append(results)

    def work_complete(self):
        print 'tabulating results!'
        print self._finished