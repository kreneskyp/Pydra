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

from django.db import models

import dbsettings
from dbsettings.loading import set_setting_value


""" ================================
Settings
================================ """
class PydraSettings(dbsettings.Group):
    host        = dbsettings.StringValue('host', 'IP Address or hostname for this server.  This value will be used by all nodes in the cluster to connect')
    port        = dbsettings.IntegerValue('port','Port for this server')
pydraSettings = PydraSettings('Pydra')

# set defaults for settings
if not pydraSettings.host:
    set_setting_value('pydra_server.models', '', 'host', 'localhost')
if not pydraSettings.port:
    set_setting_value('pydra_server.models', '', 'port', 18800)


""" ================================
Models
================================ """

"""
 Represents a node in the cluster
"""
class Node(models.Model):
    host            = models.CharField(max_length=255)
    port            = models.IntegerField(default=11880)
    cores_available = models.IntegerField(null=True)
    cores           = models.IntegerField(null=True)
    key             = models.CharField(max_length=50, null=True)

    cpu_speed       = models.IntegerField(null=True)
    memory          = models.IntegerField(null=True)
    seen            = models.IntegerField(default=False)

    # non-model fields
    ref             = None
    _info           = None

    def __str__(self):
        return '%s:%s' % (self.host, self.port)

    def status(self):
        ret = 1 if self.ref else 0
        return ret


"""
Custom manager overridden to supply pre-made queryset for queued and running tasks
"""
class TaskInstanceManager(models.Manager):
    def queued(self):
        return self.filter(started=None)

    def running(self):
        return self.filter(completed=None).exclude(started=None)


"""
Represents and instance of a Task.  This is used to track when a Task was run
and whether it completed.
"""
class TaskInstance(models.Model):
    task_key        = models.CharField(max_length=255)
    subtask_key     = models.CharField(max_length=255, null=True)
    args            = models.TextField(null=True)
    queued          = models.DateTimeField(auto_now_add=True)
    started         = models.DateTimeField(null=True)
    completed       = models.DateTimeField(null=True)
    worker          = models.CharField(max_length=255, null=True)
    #completion_type = models.IntegerField(null=True)

    objects = TaskInstanceManager()
