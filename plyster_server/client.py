from twisted.spread import pb
from twisted.internet import reactor

class EchoClient(object):
    def connect(self):
        clientfactory = pb.PBClientFactory()
        reactor.connectTCP("localhost", 8789, clientfactory)
        d = clientfactory.getRootObject()
        d.addCallback(self.send_msg)


    def send_msg(self, result):
        d = result.callRemote("echo", "hello network")
        d.addCallback(self.get_msg)

    def get_msg(self, result):
        print "server echoed: ", result

if __name__ == '__main__':
    EchoClient().connect()
    reactor.run()
