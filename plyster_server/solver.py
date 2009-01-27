"""
solver.py
Solves one portion of a problem, in a separate process on a separate CPU
"""
import sys, random, math
from twisted.spread import pb
from twisted.internet import reactor

class Solver(pb.Root):

    def __init__(self, id):
        self.id = id

    def __str__(self): # String representation
        return "Solver %s" % self.id

    def remote_initialize(self, initArg):
        return "%s initialized" % self

    def step(self, arg):
        "Simulate work and return result"
        result = 0
        for i in range(random.randint(1000000, 3000000)):
            angle = math.radians(random.randint(0, 45))
            result += math.tanh(angle)/math.cosh(angle)
        return "%s, %s, result: %.2f" % (self, str(arg), result)

    # Alias methods, for demonstration version:
    remote_step1 = step
    remote_step2 = step
    remote_step3 = step

    def remote_status(self):
        return "%s operational" % self

    def remote_terminate(self):
        reactor.callLater(0.5, reactor.stop)
        return "%s terminating..." % self

if __name__ == "__main__":
    port = int(sys.argv[1])
    reactor.listenTCP(port, pb.PBServerFactory(Solver(sys.argv[1])))
    reactor.run()
