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
    set_setting_value('plyster_server.models', '', 'host', 'localhost')
if not pydraSettings.port:
    set_setting_value('plyster_server.models', '', 'port', 18800)


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


"""
Represents and instance of a Task.  This is used to track when a Task was run
and whether it completed.
"""
class TaskInstance(models.Model):
    task_key    = models.CharField(max_length=255)
    queued      = models.DateField()
    started     = models.DateField()
    completed   = models.DateField()
