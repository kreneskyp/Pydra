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

    return range(0,node.cores)

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