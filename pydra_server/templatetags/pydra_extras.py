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

from django.template.defaultfilters import stringfilter
from django import template
from django.utils.safestring import mark_safe
register = template.Library()

from pydra_server.cluster.tasks.tasks import STATUS_CANCELLED, STATUS_FAILED, STATUS_STOPPED, STATUS_RUNNING, STATUS_PAUSED, STATUS_COMPLETE


"""
Filter that returns the the number of available cores on a node
"""
@register.filter(name='available_cores')
def available_cores(node):
    if node.cores_available:
        return node.cores_available
    else:
        return node.cores
register.filter('available_cores',available_cores)


"""
Filter that creates a range equal to the number of cores available on a node
"""
@register.filter(name='node_range')
def node_range(node):
    if node.cores_available:
        return range(0,node.cores_available)

    #default to all cores
    if node.cores:
        return range(0,node.cores)

    #node hasn't been initialized, we don't know how many cores it has
    return None

register.filter('node_range',node_range)


"""
Filter that retrieves a tasks description from a dict of tasks
using task_key to look it up
"""
@register.filter(name='task_description')
def task_description(tasks, key):
    return tasks[key].description
register.filter('task_description',task_description)


"""
Filter that replaces a task code with an icon
"""
@register.filter(name='task_status')
def task_status(code):
    if code == STATUS_RUNNING:
        css_class = "task_status_running"
        title = "task running"

    elif code == STATUS_COMPLETE:
        css_class = "task_status_complete"
        title = "task completed succesfully"

    elif code == STATUS_STOPPED:
        css_class = "task_status_stopped"
        title = "task is queued, but has not started yet"

    elif code == STATUS_CANCELLED:
        css_class = "task_status_cancelled"
        title = "task cancelled by user"

    elif code == STATUS_FAILED:
        css_class = "task_status_failed"
        title = "task failed"

    elif code == STATUS_PAUSED:
        css_class = "task_status_paused"
        title = "task paused"

    else:
        css_class = "task_status_unknown"
        title = "unknown status"

    return mark_safe('<div class="icon %s" title="%s"></div>' % (css_class, title))

register.filter('task_status', task_status)


"""
Filter that renders string with no escaping
"""
@register.filter(name='no_escape')
def no_escape(string):
    return mark_safe(string)
register.filter('no_escape', no_escape)

