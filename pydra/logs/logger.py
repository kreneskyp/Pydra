"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""

from __future__ import with_statement

import logging
import logging.handlers
from logging import FileHandler
import os
from threading import Lock

import pydra_settings as settings

LOG_FORMAT = "%%(asctime)s [%%(levelname)s] %s %%(message)s"
LOG_FORMAT_TASK = "%(asctime)s [%(levelname)s] %(message)s"

INITED_FILES = []
INIT_LOCK = Lock()

def init_logging(filename, component=''):
    """
    Utility function that configures the root logger so classes that require
    logging do not have to implement all this code.  After executing this
    function the calling function/class the logging class can be used with
    the root debugger.  ie. logging.info('example')

    This function records initialized loggers so that they are not initalized
    more than once, which would result in duplicate log messages
    
    @param filename - filename to open
    @param component - an additional string to prepend to all messages.  useful
                    for Node and Worker that log to the same file
    """

    global INITED_FILES
    global INIT_LOCK

    with INIT_LOCK:

        logger = logging.getLogger('root')

        # only init a log once.
        if filename in INITED_FILES:
            return logger

        # set up logger  
        logger.setLevel(settings.LOG_LEVEL)

        handler = logging.handlers.RotatingFileHandler(
                 filename, 
                 maxBytes    = settings.LOG_SIZE, 
                 backupCount = settings.LOG_BACKUP)
        handler.setLevel(settings.LOG_LEVEL)

        formatter = logging.Formatter(LOG_FORMAT % component)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        INITED_FILES.append(filename)

    return logger


def task_log_path(task, subtask=None, workunit=None, worker=None):
    """
    Generates the full path to the logfile for a task.
    
    @param task - id of task
    @param subtask - task path to subtask, default = None
    @param workunut - workunit key, default = None
    @param worker - worker the log is stored on.  default=None.  If no worker is
                    supplied it is assumed to be the path to the log archive
                    which stores all task logs together.  where on a node the
                    logs are stored by worker.
    """
    log_dir = settings.LOG_DIR[:-1] if settings.LOG_DIR.endswith('/') else \
        settings.LOG_DIR
    if worker:    
        dir = '%s/worker.%s' % (log_dir, worker)
    else:
        dir = settings.LOG_ARCHIVE
    
    if workunit:
        return \
            '%s/%s/' % (dir, task), \
            '%s/%s/workunit.%s.%s.log' % (dir, task, subtask, workunit)
    else:    
        return \
            '%s/%s/' % (dir, task), \
            '%s/%s/task.log' % (dir, task)


def get_task_logger(worker, task, subtask=None, workunit=None):
    """
    Initializes a logger for tasks and subtasks.  Logs for tasks are stored as
    in separate files and aggregated.  This allow workunits to be viewed in a
    single log.  Otherwise a combined log could contain messages from many 
    different workunits making it much harder to grok.

    @param worker: there may be more than one Worker per Node.  Logs are
                      stored per worker.
    @param task: ID of the task instance.  Each task instance receives 
                             its own log.
    @param subtask: (optional) subtask_key.  see workunit_id
    @param workunit: (optional) ID of workunit.  workunits receive their
                         own log file so that the log can be read separately.
                         This is separate from the task instance log.
    """
    directory, filename = task_log_path(task, subtask, workunit, worker)
    os.makedirs(directory)

    logger_name = 'task.%s' % task

    if workunit:
        logger_name += '.%s' % workunit

    logger = logging.getLogger(logger_name)
    handler = FileHandler(filename)
    
    formatter = logging.Formatter(LOG_FORMAT_TASK)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(settings.LOG_LEVEL)

    return logger
