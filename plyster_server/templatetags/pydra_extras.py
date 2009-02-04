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