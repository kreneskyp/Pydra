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

import os
import shutil
import time
import unittest
from datetime import datetime

#environment must be configured before loading tests
from pydra.config import configure_django_settings
configure_django_settings()
import pydra_settings

from pydra.cluster.tasks import TaskNotFoundException, packaging
from pydra.cluster.tasks.task_manager import TaskManager
from pydra.models import TaskInstance
from pydra.util import makedirs

class TaskManager_Test(unittest.TestCase):

    def setUp(self):
        self.tasks = [
                'demo.demo_task.TestTask',
                'demo.demo_task.TestContainerTask',
                'demo.demo_task.TestParallelTask'
                ]
        self.completion = {}
        for task in self.tasks:
            self.completion[task] = None

        # setup manager with an internal cache we can alter
        self.manager = TaskManager(None, lazy_init=True)
        pydra_settings.TASK_DIR_INTERNAL = '/var/lib/pydra/test_tasks_internal'

        # find at least one task package to use for testing
        self.package = 'demo'
        self.package_dir = '%s/%s' % (self.manager.tasks_dir_internal, self.package)

        self.task_instances = []
        for task in self.tasks [:2]:
            #queued tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.save()
            self.task_instances.append(task_instance)

            #running tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            task_instance.save()
            self.task_instances.append(task_instance)

            #finished tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            completed_time = datetime.now()
            task_instance.completed = completed_time
            task_instance.save()
            self.completion[task] = completed_time
            self.task_instances.append(task_instance)

            #failed tasks
            task_instance = TaskInstance()
            task_instance.task_key=task
            task_instance.started = datetime.now()
            task_instance.status = -1
            task_instance.save()
            self.task_instances.append(task_instance)


    def tearDown(self):
        for task in self.task_instances:
            task.delete()
        self.clear_cache()
        os.removedirs(self.manager.tasks_dir_internal)


    def create_cache_entry(self, hash='FAKE_HASH'):
        """
        Creates an entry in the task_cache_internal
        """
        internal_folder = os.path.join(self.manager.tasks_dir_internal,
                    self.package, hash)
        pkg_dir = '%s/%s' % (pydra_settings.TASKS_DIR, self.package)

        makedirs(pkg_dir)
        shutil.copytree(pkg_dir, internal_folder)
        
        
    def clear_cache(self):
        """
        Clears the entire cache of all packages
        """
        self.clear_package_cache()
        if os.path.exists(self.package_dir):
            shutil.rmtree(self.package_dir)
        
        
    def clear_package_cache(self):
        """
        Cleans just the cached versions of the selected task
        """
        if os.path.exists(self.package_dir):
            for version in os.listdir(self.package_dir):
                shutil.rmtree('%s/%s' % (self.package_dir, version))


    def test_trivial(self):
        """
        Test the basic init and teardown of the test harness and TaskManager.
        """
        pass


    def test_listtasks(self):
        """
        Tests list tasks function to verify it returns all the tasks that it should
        """
        self.create_cache_entry()
        self.manager.init_task_cache()
        tasks = self.manager.list_tasks()
        self.assertEqual(len(tasks), 6, "There should be 3 registered tasks")

        for task in self.tasks:
            self.assert_(tasks.has_key(task), 'Task is missing from list tasks')
            recorded_time = self.completion[task]
            recorded_time = time.mktime(recorded_time.timetuple()) if recorded_time else None
            list_time = tasks[task]['last_run']
            self.assertEqual(recorded_time, list_time, "Completion times for task don't match: %s != %s" % (recorded_time, list_time))


    def test_init_cache_empty_cache(self):
        self.manager.init_task_cache()
        self.assertEqual(len(self.manager.registry), 0, 'Cache is empty, but registry is not')


    def test_init_cache(self):
        self.create_cache_entry()
        self.manager.init_task_cache()
        package = self.manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')


    def test_init_package(self):
        self.create_cache_entry()
        self.manager.init_task_cache()
        length = len(self.manager.registry)
        package = self.manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_init_package_empty_package(self):
        os.mkdir(self.package_dir)
        self.assertRaises(TaskNotFoundException, self.manager.init_package, self.package)
        self.assertEqual(len(self.manager.registry), 0, 'Cache is empty, but registry is not')

    def test_init_package_multiple_versions(self):
        self.create_cache_entry('FAKE_HASH_1')
        self.create_cache_entry('FAKE_HASH_2')
        self.create_cache_entry('FAKE_HASH_3')
        self.manager.init_package(self.package)
        length = len(self.manager.registry)
        package = self.manager.registry[(self.package, 'FAKE_HASH_3')]
        self.assertNotEqual(package, None, 'Registry does not contain latest package')
        try:
            package = None
            package = self.manager.registry[(self.package, 'FAKE_HASH_2')]
        except KeyError:
            pass
        self.assertEqual(package, None, 'Registry contains old package')

    def test_autodiscover(self):
        self.fail('Not Implemented')

    def test_add_package(self):
        self.create_cache_entry()
        package = packaging.TaskPackage(self.package, self.package_dir, 'FAKE_HASH')
        self.manager._add_package(package)
        package = self.manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')

    def test_add_package_with_dependency(self):
        self.create_cache_entry()
        package = packaging.TaskPackage(self.package, self.package_dir, 'FAKE_HASH')
        self.manager._add_package(package)
        package = self.manager.registry[(self.package, 'FAKE_HASH')]
        self.assertNotEqual(package, None, 'Registry does not contain package')
        self.fail('Not Implemented')


    def test_add_package_with_missing_dependency(self):
        self.fail('Not Implemented')

    def test_retrieve_task(self):
        self.create_cache_entry()
        self.manager.init_task_cache()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')

    def test_lazy_init(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')

    def test_lazy_init_with_dependency(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')
        self.fail('Not Implemented')

    def test_lazy_init_with_missing_dependency(self):
        self.create_cache_entry()
        helper = RetrieveHelper()
        task_key = 'demo.demo_task.TestTask'
        self.manager.retrieve_task(task_key,'FAKE_HASH', helper.callback, \
                                   helper.errback)
        self.assertEquals(task_key, helper.task_key , 'Task_key does not match')
        self.assertEquals('FAKE_HASH', helper.version , 'Task_key does not match')
        self.fail('Not Implemented')


class RetrieveHelper():
    task_key = None
    version = None
    task_class= None
    module_path = None
    args = None
    kwargs = None
    
    def callback(self, task_key, version, task_class, module_path, *args, **kw):
        self.task_key = task_key
        self.version = version
        self.task_class = task_class
        self.module_path = module_path
        self.args = args
        self.kwargs = kw
        
    def errback(self):
        pass

if __name__ == "__main__":
    unittest.main()
