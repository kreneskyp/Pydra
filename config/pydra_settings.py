# Django settings for pydra project.
VERSION = '0.0.1'

DEBUG = True
TEMPLATE_DEBUG = DEBUG

# Base directory for storing all files created at runtime
# this includes encryption keys, logs, tasks, etc
RUNTIME_FILES_DIR = '/var/lib/pydra'

# Directory where process ids are stored.
RUNTIME = '/var/run/pydra'



DATABASE_ENGINE = 'mysql'      # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
DATABASE_NAME = 'pydra'        # Or path to database file if using sqlite3.
DATABASE_USER = 'pydra'        # Not used with sqlite3.
DATABASE_PASSWORD = 'pydra'    # Not used with sqlite3.
DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.


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

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'dk#^frv&4y_&7a90#bn62@t-1jyc@q9*!69y7zq&@&8)g#szu4'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
)

INSTALLED_APPS = (
    'pydra'
)

#Logging
import logging
LOG_LEVEL = logging.DEBUG
LOG_DIR = '/var/log/pydra'
LOG_FILENAME_MASTER = '%s/master.log' % LOG_DIR
LOG_FILENAME_NODE   = '%s/node.log' % LOG_DIR
LOG_SIZE = 10000000
LOG_BACKUP = 10


# Connection settings for cluster.   HOST is the name that pydra will be exposed
# to and used as part of an identifier.  PORT is the port that Node will
# listen on for the Master to connect.  CONTROLLER_PORT is the port controllers
# can connect to the Master on.
HOST = 'localhost'
PORT = 18800
CONTROLLER_PORT = 18801


# Directory in which to place tasks to deploy to Pydra.  Tasks will be parsed
# and versioned.  Processed Tasks are stored in TASKS_DIR_INTERNAL.  Modifying
# files within TASKS_DIR_INTERNAL _WILL_ break things.
TASKS_DIR = '%s/tasks' % RUNTIME_FILES_DIR
TASKS_DIR_INTERNAL = '%s/tasks_internal' % RUNTIME_FILES_DIR


# Automatically add nodes found with autodiscovery 
MULTICAST_ALL = False 
