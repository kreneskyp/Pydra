#!/usr/bin/env python
from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES
import os

def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join) in a
    platform-neutral way.
    
    shamelessly borrowed from django
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


# Tell distutils to put the data_files in platform-specific installation
# locations. See here for an explanation:
# http://groups.google.com/group/comp.lang.python/browse_thread/thread/35ec7b2fed36eaec/2105ee4d9e8042cb
for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']


# Compile the list of packages available, because distutils doesn't have
# an easy way to do this.
#
# shamelessly borrowed from django
packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
    os.chdir(root_dir)
pydra_dir = 'pydra'

for dirpath, dirnames, filenames in os.walk(pydra_dir):
    # Ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith('.'): del dirnames[i]
    if '__init__.py' in filenames:
        packages.append('.'.join(fullsplit(dirpath)))
    elif filenames:
        data_files.append([dirpath, [os.path.join(dirpath, f) for f in filenames]])

# add config, init.d, and other files
data_files.append( ['/etc/pydra', ['config/pydra_settings.py']] )
data_files.append( ['/etc/init.d', ['scripts/pydra_master', 'scripts/pydra_node']] )
data_files.append( ['/usr/bin', ['scripts/pydra_manage']] )

setup(name='Pydra',
      version='0.01',
      description='Distributed computing framework',
      long_description = 'Pydra is a framework for distributing and parallizing work to a cluster of computers.',
      maintainer='Peter Krenesky',
      maintainer_email='peter@osuosl.org',
      url='http://pydra-project.osuosl.org',
      packages=packages,
      data_files=data_files,
      requires = ['twisted','django','pyamf','simplejson'],
      provides = ['pydra.web'],
      license='GPLv3'
     )

 
