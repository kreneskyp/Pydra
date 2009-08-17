# Django settings for pydra project.

VERSION = '0.01'

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

DATABASE_ENGINE = 'mysql'           # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
DATABASE_NAME = 'pydra'             # Or path to database file if using sqlite3.
DATABASE_USER = 'pydra'             # Not used with sqlite3.
DATABASE_PASSWORD = 'pydra'         # Not used with sqlite3.
DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.


# prefix used for the site.  ie. http://myhost.com/<SITE_ROOT>/
# for the django standalone server this should be /
# for apache this is the url the site is mapped to, probably /pgd
SITE_ROOT = ''

# absolute path to the docroot of this site
DOC_ROOT = '/home/peter/wrk/pydra/pydra_site'

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = '%s/static' % DOC_ROOT

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = '%s/static' % SITE_ROOT

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
ADMIN_MEDIA_PREFIX = '/media'

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'dk#^frv&4y_&7a90#bn62@t-1jyc@q9*!69y7zq&@&8)g#szu4'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
)

#Logging
import logging
LOG_LEVEL = logging.DEBUG
LOG_DIR = '/var/log/pydra'
LOG_FILENAME_MASTER = '%s/master.log' % LOG_DIR
LOG_FILENAME_NODE   = '%s/node.log' % LOG_DIR
LOG_SIZE = 10000000
LOG_BACKUP = 10




MIDDLEWARE_CLASSES = (
)

ROOT_URLCONF = 'urls'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

INSTALLED_APPS = (
    'pydra'
)


# pydra settings
HOST = 'localhost'
PORT = 18800
CONTROLLER_PORT = 18801 
TASKS_DIR = '/var/lib/pydra/tasks'
MULTICAST_ALL = False #Automatically use all the nodes found
