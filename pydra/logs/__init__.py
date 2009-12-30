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
from pydra.logs.logger import init_logging, get_task_logger


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
    if worker:    
        dir = '%s/%s' (pydra_settings.LOG_DIR, worker)
    else:
        dir = pydra_settings.LOG_ARCHIVE_DIR
    
    if workunit:
        return '%s/task.%s/workunit.%s.%s.log' % (dir, task, subtask, workunit)
    else:    
        return '%s/task.%s/task.log' % (dir, task)