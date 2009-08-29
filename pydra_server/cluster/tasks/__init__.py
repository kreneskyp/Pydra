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

STATUS_CANCELLED = -2;
STATUS_FAILED = -1;
STATUS_STOPPED = 0;
STATUS_RUNNING = 1;
STATUS_PAUSED = 2;
STATUS_COMPLETE = 3;

class TaskNotFoundException(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


from tasks import Task
from parallel_task import ParallelTask
from task_container import TaskContainer
