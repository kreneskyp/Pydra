

from threading import Thread
import time


class Foo(Thread):
    def run(self):
        while True:
            s = 1+2+3+4+5

l = []
for i in range(3):
    foo = Foo()
    foo.start()
    l.append(foo)

time.sleep(20)