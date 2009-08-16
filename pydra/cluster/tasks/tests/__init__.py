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

import unittest

from pydra_server.cluster.tasks.tests.task_manager import suite as task_manager_suite
from pydra_server.cluster.tasks.tests.tasks import suite as task_suite


def suite():
    """
    Build a test suite from all the test suites in tasks
    """
    tasks_suite = unittest.TestSuite()
    tasks_suite.addTest(task_manager_suite())
    tasks_suite.addTest(task_suite())

    return tasks_suite