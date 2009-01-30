from django.conf.urls.defaults import *

urlpatterns = patterns('',
    (r'^$', jobs),
    (r'^nodes/$', nodes),
    (r'^nodes/edit/(\d?)$', nodes_edit),
    (r'^jobs/$', jobs),
)
