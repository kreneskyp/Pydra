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


from django.conf.urls.defaults import *

from views import *
from django.contrib.auth.views import login, logout

from pydra.config import load_settings
load_settings()

urlpatterns = patterns('',
    #default
    (r'^$', jobs),

    # node urls
    (r'^nodes/$', nodes),
    (r'^nodes/discover/$', discover),
    (r'^nodes/edit/(\d?)$', node_edit),
    (r'^nodes/delete/(\d?)$', node_delete),
    (r'^nodes/status/$', node_status),

    # job urls
    (r'^jobs/$', jobs),
    (r'^jobs/run/$', run_task),
    (r'^jobs/cancel/$', cancel_task),
    (r'^jobs/progress/$', task_progress),

    # job history urls
    (r'^jobs/history/$', task_history), 
    (r'^jobs/history/detail/$', task_history_detail), 

    #authentication
    (r'^accounts/login/$', login),
    (r'^accounts/logout/$', logout, {'next_page':'/jobs'}),
    (r'^accounts/profile/$', jobs),
)

# The following is used to serve up local media files like images, css, js
# Use __file__ to find the absolute path to this file.  This can be used to
# determine the path to the static directory which contains all the files
# we are trying to expose
media_root = '%s/static' % __file__[:__file__.rfind('/')]
baseurlregex = r'^static/(?P<path>.*)$'
urlpatterns += patterns('',
    (baseurlregex, 'django.views.static.serve', {'document_root': media_root})
)
