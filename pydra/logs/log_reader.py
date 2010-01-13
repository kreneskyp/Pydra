#!/usr/bin/python

import sys, getopt, warnings

from pydra.config import *

# loads settings and other one-time config stuff
def setup():
    load_settings()
    configure_django_settings()

    from pydra.logs.log_aggregator import MasterLogAggregator     

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "ah", ["all", "help"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    setup()

def usage():
    print "Usage: log_reader.py [ARGS]"
    print "Try 'log_reader.py --help' for more information."    

def help():
    print "Help Message Goes Here"

if __name__ == "__main__":
    main(sys.argv[1:])
