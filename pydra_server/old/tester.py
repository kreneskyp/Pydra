import time
from pyamf.remoting.client import RemotingService

client = RemotingService('http://127.0.0.1:18801')
client.setCredentials('controller','1234')

service = client.getService('controller')

for i in range(1):
    print service.is_alive()
    time.sleep(2)
