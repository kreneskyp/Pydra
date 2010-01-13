from pydra.cluster.tasks import Task

class Fail(Task):
    #syntax fail
    foo = bar
    
    def __init__(self):
        fake + die
