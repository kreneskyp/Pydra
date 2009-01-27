"""
Example Parralel Task
"""
from tasks import ParallelTask


class PTask(ParallelTask):
    count = 0
    stop = 20

    def __init__(self, msg='Demo Task'):
        ParallelTask.__init__(self, msg)

    """
    Does some simple 'work'
    """
    def _work(self, args=None):
        if args == None:
            args = {'testValue':0}
        if not args.has_key('testValue'):
            args['testValue'] = 0

        while self.count < self.stop:
            time.sleep(1)
            self.count += 1
            args['testValue'] += 1
            print 'value: %d   progress: %d%%' % (args['testValue'] , self.progress())

        return self.count

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
