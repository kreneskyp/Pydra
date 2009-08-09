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
import os

def init_dir(path):
    """
    Initializes a path to ensure that all directories in that path are created.

    Will iterate through all directories in the path checking the existence of
    each one.  Any directories that do not exist will be created.
    """
    
    if path.startswith('./'):
        prefix = './'
        unprefixed_path = path[2:]
    elif path.startswith('/'):
        prefix = '/'
        unprefixed_path = path[1:]
    else:
        unprefixed_path = path
        prefix = ''

    if path.endswith('/'):
        unprefixed_path = unprefixed_path[:-1]

    paths = unprefixed_path.split('/')
    dir_path = '%s%s' % (prefix, paths[0])
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    for directory in paths[1:]: 
        dir_path = '%s/%s' % (dir_path, directory)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
