#!/usr/bin/python

import os, sys, getopt, warnings
from pydra.config import *
settings = load_settings()
from pydra.logs.logger import *
warnings.simplefilter('ignore', UserWarning)

def get_all_logs():
    files = os.listdir(settings.LOG_ARCHIVE)
    files.sort()
    return files    

def print_logs(ids):
    ts = {}
    for id in ids:
        try:
            # look up log dir and main task.log file
            dir, logfile = task_log_path(id)
            
            # get all extra workunit files in dir
            files = os.listdir(dir)
            files.sort()
            files.remove('task.log')
            for f in files:
                file = open(dir+f, 'r')
                line = file.readline()
                line = line.split(' ', 2)
                line = "%s %s" % (line[0], line[1])
                file.seek(0)
                ts[line] = file                 
            
            file = open(logfile, 'r')
            
            print_header(id, logfile)
            for line in file:
                timestamp = line.split(' ', 2)
                timestamp = "%s %s" % (timestamp[0], timestamp[1])
                
                for key in ts.keys():
                    if key < timestamp:
                        print "\t************"
                        print "\tWorkunit Log"
                        print "\t************"
                        for l in ts[key]:
                            print "\t"+l,
                        del ts[key]                

                print line,

        except IOError:
            print "FILE NOT FOUND!!!"
            print "Are the logs aggregated? Is the task ID correct?\n"

def print_header(id, filename):
    print "=============================================================================="
    print "TASK ID: %d" % (id)
    print "=============================================================================="

def main(argv):
    ids = []

    try:
        opts, args = getopt.getopt(argv, "ah", ["all", "help"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            help()
            sys.exit()
        if opt in ("-a", "--all"):
            args = get_all_logs()
    
    for id in args:
        try:
            if not (int(id)) in ids:
                ids.append(int(id))
        except ValueError:
            usage()
            sys.exit(2)

    print_logs(ids)

def usage():
    print "Usage: log_reader.py [OPTION] [ARGS]"
    print "Try 'log_reader.py --help' for more information."    

def help():
    print "Usage: log_reader.py [OPTIONS] [ARGS]"
    print "Prints logs for task ids ARGS with OPTIONS"
    print "ARGS: list of task ids - '1, 2, 3'"
    print "OPTIONS:"
    print "   -a --all : gets all task logs, could be huge"
    print "   -h --help: prints this help message"
    print "Example: log_reader.py 1 3 6" 

if __name__ == "__main__":
    main(sys.argv[1:])
