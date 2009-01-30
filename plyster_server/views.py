from django.shortcuts import render_to_response
from django.template import RequestContext
from django.core.paginator import Paginator, InvalidPage, EmptyPage
import math

from pgd_search.models import searchSettings

"""
display nodes
"""
def nodes(request):
    # get nodes
    nodes = Node.objects.all()    

    #paginate
    segments = search.querySet().order_by('protein')
    paginator = Paginator(nodes, 25) # Show 25 segments per page

    # Make sure page request is an int. If not, deliver first page.
    try:
        page = lint(request.GET.get('page', '1'))
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
        'nodes':paginatedNode,
        'pages':pages,
    }, context_instance=RequestContext(request))


"""
Handler for creating and editing nodes
"""
def node_edit(request, id=None):
    if request.method == 'POST': # If the form has been submitted
        if id:
            node = Node(pk=id)
            form = NodeForm(request.POST, node) # A form bound to the POST data
        else:
            form = NodeForm(request.POST) # A form bound to the POST data

        if form.is_valid(): # All validation rules pass
                   
            form.save()

            return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = Node(pk=id)
            form = NodeForm(node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
    }, context_instance=RequestContext(request))


