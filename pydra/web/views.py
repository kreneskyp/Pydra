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
import math, re

from django.contrib.auth import authenticate, login
from django.contrib.auth.decorators import user_passes_test
from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.utils import simplejson
import pydra_web_settings as settings


from pydra.cluster.controller import ControllerException
from pydra.cluster.controller.web.controller import WebController
from pydra.forms import NodeForm
from pydra.models import Node, TaskInstance
from pydra.config import load_settings

"""
pydraController is a global variable that stores an instance of a Controller.
The current controller does not involve much setup so this may not be required
any longer, but it will improve resource usage slightly
"""
pydra_controller = WebController(settings.HOST, \
                            settings.CONTROLLER_PORT,
                                key='%s/master.key' % settings.RUNTIME_FILES_DIR
)


def pydra_processor(request):
    """
    Pydra_processor is used by any view that will need information or control
    over the master server
    """
    global pydra_controller
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
        nodes, pages = pydra_controller.node_list()
    except ControllerException, e:
        response = e.code
        nodes = None
        pages = None

    return render_to_response('nodes.html', {
        'nodes':nodes,
        'pages':pages,
    }, context_instance=c)


@user_passes_test(lambda u: u.has_perm('pydra.web.can_edit_nodes'))
def node_delete(request, id=None):
    """
    Handler for deleting nodes
    """
    c = RequestContext(request, processors=[pydra_processor, settings_processor])

    if id: 
        pydra_controller.node_delete(id)
        return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = pydra_controller.node_detail(id)
            form = NodeForm(node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
        'id':id,
    }, context_instance=c)


@user_passes_test(lambda u: u.has_perm('pydra.web.can_edit_nodes'))
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
            pydra_controller.node_edit(form.cleaned_data)

            return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = pydra_controller.node_detail(id)
            form = NodeForm(node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
        'id':id,
    }, context_instance=c)


@user_passes_test(lambda u: u.has_perm('pydra.web.can_edit_nodes'))
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
            pydra_controller.connect()
    return render_to_response('discover.html', {'known_nodes': pydra_controller.list_known_nodes()})


def node_status(request):
    """
    Retrieves Status of nodes
    """
    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])

    try:
        response = simplejson.dumps(pydra_controller.node_status())
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')


def jobs(request):
    """
    handler for displaying jobs
    """
    error = None

    try:
        tasks = pydra_controller.list_tasks()
    except ControllerException, e:
        tasks = None
        error = e.code

    try:
        queue = pydra_controller.list_queue()
    except ControllerException, e:
        queue = None
        error = e.code

    return render_to_response('tasks.html', {
        'MEDIA_URL': settings.MEDIA_URL,
        'tasks': tasks if tasks else None,
        'queue': queue if queue else None,
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
        history = pydra_controller.task_history(request.GET['key'], page)
    except ControllerException, e:
        history = None
        error = e.code

    return render_to_response('task_history.html', {
        'MEDIA_URL': settings.MEDIA_URL,
        'history':   history,
        'task_key':  request.GET['key'],
        'controller_error': error
    }, context_instance=c)


def task_history_detail(request):
    c = RequestContext(request, processors=[pydra_processor,
            settings_processor])

    error = None
    id = request.GET['id']
    try:
        detail = pydra_controller.task_history_detail(id)
    except ControllerException, e:
        detail = None
        error = e.code

    return render_to_response('task_history_detail.html', {
        'task': detail,
        'task_id': request.GET['id'],
        'controller_error': error
    }, context_instance=c)


LOG_LEVELS = (
    (re.compile('^.{23} \[ERROR\].*'), 'error'),
    (re.compile('^.{23} \[WARN\].*'), 'warning'),
    (re.compile('^.{23} \[INFO\].*'), 'info'),
    (re.compile('^.{23} \[DEBUG\].*'), 'debug'),
)
def task_log(request):
    """
    Handler for retrieving workunit logs 
    """
    re_workunit = re.compile("\*{3} Workunit Started \- ([\S]+):(\d+) \*{3}$")
    
    c = RequestContext(request, {
    }, [pydra_processor])
    log = []
        
    task_id = request.GET['task_id']
    if 'subtask' in request.GET and 'workunit_id' in request.GET:
        args = (task_id, request.GET['subtask'], request.GET['workunit_id'])
        template = 'log_lines.html'
    else:
        args = (task_id,)
        template = 'log.html'
    
    try:
        data = pydra_controller.task_log(*args)
    except ControllerException, e:
        data = e.code

    for line in data.split('\n'):
        if line:
            match = re_workunit.search(line)
            for exp, css_class in LOG_LEVELS:
                if exp.match(line):
                    break
            if match:
                log.append({'subtask':match.group(1),
                            'task_id':task_id,
                            'workunit_id':match.group(2),
                            'line':line,
                            'css':css_class})
            else:
                log.append({'line':line, 'css':css_class})

    return render_to_response(template, { 'log': log, });
 

def task_progress(request):
    """
    Handler for retrieving status 
    """
    c = RequestContext(request, {
    }, [pydra_processor])

    try:
        data = simplejson.dumps(pydra_controller.task_statuses())
    except ControllerException, e:
        data = e.code

    return HttpResponse(data, mimetype='application/javascript');


@user_passes_test(lambda u: u.has_perm('pydra.web.can_run'))
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
        response = simplejson.dumps(pydra_controller.queue_task(key, args))
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')


@user_passes_test(lambda u: u.has_perm('pydra.web.can_run'))
def cancel_task(request):
    """
    handler for sending a cancel_task signal
    """
    id = request.POST['i']

    c = RequestContext(request, {
    }, [pydra_processor])

    try:
        response = pydra_controller.cancel_task(id)
    except ControllerException, e:
        response = e.code

    return HttpResponse(response, mimetype='application/javascript')
