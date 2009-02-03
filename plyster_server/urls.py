from django.conf.urls.defaults import *

from views import *

urlpatterns = patterns('',
    (r'^$', jobs),
    (r'^nodes/$', nodes),
    (r'^nodes/edit/(\d?)$', node_edit),
    (r'^nodes/status/$', nodes_status),
    (r'^jobs/$', jobs),
)
