"""
mod_wsgi script - this script is used by mod_wsgi to wrap the django site
                  making it able to process it.
"""

# redirect output
import sys
sys.stdout = sys.stderr

# configure pydra environment
from pydra.config import configure_django_settings
configure_django_settings()

# setup wsgi
import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()
