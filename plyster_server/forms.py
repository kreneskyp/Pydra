from django import forms
from django.forms import ModelForm

from models import Node

"""
Form used when creating nodes
"""
class NodeForm(ModelForm):
    class Meta:
        model = Node
        exclude=('key')

    cores_available = forms.IntegerField(required=False)
    cores           = forms.IntegerField(required=False)    
    cpu_speed       = forms.IntegerField(required=False)
    memory          = forms.IntegerField(required=False)
