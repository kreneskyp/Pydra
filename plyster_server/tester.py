from pyamf.remoting.client import RemotingService

gw = RemotingService('http://127.0.0.1:8050')
service = gw.getService('controller')

print service.start()
