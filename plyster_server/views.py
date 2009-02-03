from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect
from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.utils import simplejson

import math

from plyster_server.models import Node, pydraSettings
from forms import NodeForm
import settings

"""
display nodes
"""
def nodes(request):
    # get nodes
    nodes = Node.objects.all()    

    # Get the master control interface
    master = {'running':False}

    # paginate
    paginator = Paginator(nodes, 25) # Show 25 segments per page

    # Make sure page request is an int. If not, deliver first page.
    try:
        page = int(request.GET.get('page', '1'))
    except ValueError:
        page = 1

    # If page request (9999) is out of range, deliver last page of results.
    try:
        paginatedNodes = paginator.page(page)
    except (EmptyPage, InvalidPage):
        page = paginator.num_pages
        paginatedNodes = paginator.page(page)

    #generate a list of pages to display in the pagination bar
    pages = ([i for i in range(1, 11 if page < 8 else 3)],
            [i for i in range(page-5,page+5)] if page > 7 and page < paginator.num_pages-6 else None,
            [i for i in range(paginator.num_pages-(1 if page < paginator.num_pages-6 else 9), paginator.num_pages+1)])

    return render_to_response('nodes.html', {
        'nodes':paginatedNodes,
        'pages':pages,
        'master':master,
    }, context_instance=RequestContext(request))


"""
Handler for creating and editing nodes
"""
def node_edit(request, id=None):
    if request.method == 'POST': 
        if id:
            print id
            node = Node(pk=id)
            form = NodeForm(request.POST, instance=node) 
        else:
            form = NodeForm(request.POST)

        if form.is_valid():
            form.save()
            return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = Node.objects.get(pk=id)
            form = NodeForm(instance=node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
        'id':id,
    }, context_instance=RequestContext(request))




pydraController = Controller()

def pydra_processor(request):
    global pydraController

    if pydraController == None:
        pydraController = Controller()

    if not taskClient.connected:
        taskClient.connect(5800)

    return {}


"""
Retrieves Status of nodes
"""
def nodes_status(request):
    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])

    return HttpResponse(pydraController.nodes_status(), mimetype='application/javascript')


def jobs(request):
    pass

