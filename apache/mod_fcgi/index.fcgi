"""
    Configures pydra web interface for fastcgi.
"""

# setup pydra environment
from pydra.config import configure_django_settings
configure_django_settings()

# setup fastcgi 
from django.core.servers.fastcgi import runfastcgi
runfastcgi(method="threaded", daemonize="false")