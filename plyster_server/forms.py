from django import forms
from django.forms import ModelForm


"""
Form used when creating nodes
"""
class NodeForm(ModelForm):
    class Meta:
        model = Node

