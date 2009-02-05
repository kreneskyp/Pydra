from django.conf.urls.defaults import *

from views import *

urlpatterns = patterns('',
    (r'^$', jobs),
    (r'^nodes/$', nodes),
    (r'^nodes/edit/(\d?)$', node_edit),
    (r'^nodes/status/$', node_status),
    (r'^jobs/$', jobs),
    (r'^jobs/run/$', run_task),
)



#The following is used to serve up local media files like images
#if settings.LOCAL_DEV:
baseurlregex = r'^static/(?P<path>.*)$'
urlpatterns += patterns('',
    (baseurlregex, 'django.views.static.serve', {'document_root':  settings.MEDIA_ROOT}),
)
