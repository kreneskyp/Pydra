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
    def _work(self, args=None):
        if not args:
            args = {'testValue':0}
        elif not args.has_key('testValue'):
            args['testValue'] = 0

        while self.count < self.stop:
            time.sleep(1)
            self.count += 1
            args['testValue'] += 1
            print 'value: %d   progress: %d%%' % (args['testValue'] , self.progress())

        return args

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
class MasterTestTask(TaskContainer):
    def __init__(self, msg=None):
        TaskContainer.__init__(self, msg)

        # add some children
        self.addTask(TestTask('child 1'))
        self.addTask(TestTask('child 2'))
        self.addTask(TestTask('child 3'))
