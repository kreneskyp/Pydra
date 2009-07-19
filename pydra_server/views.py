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

from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth import authenticate, login
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect
from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.utils import simplejson
from django.http import HttpResponse


import math

from pydra_server.models import Node, TaskInstance, pydraSettings
from cluster.amf.controller import AMFController, ControllerException
from forms import NodeForm
from models import pydraSettings
import settings


"""
pydraController is a global variable that stores an instance of a Controller.
The current controller does not involve much setup so this may not be required
any longer, but it will improve resource usage slightly
"""
pydra_controller = AMFController(pydraSettings.host , pydraSettings.port)


def pydra_processor(request):
    """
    Pydra_processor is used by any view that will need information or control
    over the master server
    """
    global pydra_controller

    if pydra_controller == None:
        pydra_controller = AMFController(pydraSettings.host , pydraSettings.port)

    return {'controller':pydra_controller}


def settings_processor(request):
    """
    settings_processor adds settings required by most pages
    """

    return {
        'VERSION':settings.VERSION,
        'MEDIA':settings.MEDIA_URL,
        'ROOT':settings.SITE_ROOT
    }


def nodes(request):
    """
    display nodes
    """
    c = RequestContext(request, processors=[pydra_processor, settings_processor])

    try:
        nodes, pages = pydra_controller.remote_node_list()
    except ControllerException, e:
        response = e.code
        nodes = None
        pages = None

    return render_to_response('nodes.html', {
        'nodes':nodes,
        'pages':pages,
    }, context_instance=c)


@user_passes_test(lambda u: u.has_perm('pydra_server.can_edit_nodes'))
def node_edit(request, id=None):
    """
    Handler for creating and editing nodes
    """
    c = RequestContext(request, processors=[pydra_processor, settings_processor])

    if request.method == 'POST': 
        form = NodeForm(request.POST)

        if form.is_valid():
            if id:
                form.cleaned_data['id'] = id
            pydra_controller.remote_node_edit(form.cleaned_data)

            return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = pydra_controller.remote_node_detail(id)
            form = NodeForm(node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
        'id':id,
    }, context_instance=RequestContext(request, processors=[settings_processor]))


@user_passes_test(lambda u: u.has_perm('pydra_server.can_edit_nodes'))
def discover(request):
    """
    allow users to activate the nodes that have been discovered via avahi
    """
    from django import forms
    global pydra_controller

    if request.method == 'POST':
        reconnect = False
        for i in request.POST.keys():
            host, port = i.split(':')
            if not Node.objects.filter(host=host, port=port):
                Node.objects.create(host=host, port=port)
                reconnect = True
        if reconnect:
            pydra_controller.remote_connect()
    return render_to_response('discover.html', {'known_nodes': pydra_controller.remote_list_known_nodes()})


def node_status(request):
    """
    Retrieves Status of nodes
    """
    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])

    try:
        response = simplejson.dumps(pydra_controller.remote_node_status())
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')


def jobs(request):
    """
    handler for displaying jobs
    """
    error = None

    try:
        tasks = pydra_controller.remote_list_tasks()
    except ControllerException, e:
        tasks = None
        error = e.code

    try:
        queue = pydra_controller.remote_list_queue()
    except ControllerException, e:
        queue = None
        error = e.code

    try:
        running = pydra_controller.remote_list_running()
    except ControllerException, e:
        running = None
        error = e.code

    return render_to_response('tasks.html', {
        'MEDIA_URL': settings.MEDIA_URL,
        'tasks': tasks if tasks else None,
        'queue': queue if queue else None,
        'running': running if running else None,
        'controller_error': error
    }, context_instance=RequestContext(request, processors=[pydra_processor, settings_processor]))


def task_history(request):
    c = RequestContext(request, processors=[pydra_processor, settings_processor])

    # Make sure page request is an int. If not, deliver first page.
    try:
        page = int(request.GET['page'])
    except KeyError:
        page = 1

    error = None
    try:
        history = pydra_controller.remote_task_history(request.GET['key'], page)
    except ControllerException, e:
        history = None
        error = e.code

    return render_to_response('task_history.html', {
        'MEDIA_URL': settings.MEDIA_URL,
        'history':   history,
        'task_key':  request.GET['key'],
        'controller_error': error
    }, context_instance=c)


def task_progress(request):
    """
    Handler for retrieving status 
    """
    c = RequestContext(request, {
    }, [pydra_processor])

    try:
        data = simplejson.dumps(pydra_controller.remote_task_statuses())
    except ControllerException, e:
        data = e.code

    return HttpResponse(data, mimetype='application/javascript');


@user_passes_test(lambda u: u.has_perm('pydra_server.can_run'))
def run_task(request):
    """
    handler for sending a run_task signal
    """
    key = request.POST['key']

    try:
        args = simplejson.loads(request.POST['args'])
    except KeyError:
        # task might not have args
        args = None

    c = RequestContext(request, {
    }, [pydra_processor])

    try:
        response = simplejson.dumps(pydra_controller.remote_run_task(key, args))
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')


@user_passes_test(lambda u: u.has_perm('pydra_server.can_run'))
def cancel_task(request):
    """
    handler for sending a cancel_task signal
    """
    id = request.POST['i']

    c = RequestContext(request, {
    }, [pydra_processor])

    try:
        response = pydra_controller.remote_cancel_task(id)
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')
