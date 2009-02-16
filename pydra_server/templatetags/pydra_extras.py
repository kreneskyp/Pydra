"""
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
register = template.Library()

"""
Filter that returns the full AA code from the 1 letter AA code
"""
@register.filter(name='available_cores')
def available_cores(node):
    if node.cores_available:
        return node.cores_available
    else:
        return node.cores
register.filter('available_cores',available_cores)

"""
Filter that creates a range
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
    print '===================='
    print tasks
    print key
    print '===================='
    return tasks[key].description
register.filter('task_description',task_description)