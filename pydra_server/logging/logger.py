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

import logging
import logging.handlers
import settings

def init_logging(filename):
    """
    Utility function that configures the root logger so that class that require
    logging do not have to implement all this code.  After executing this
    function the calling function/class the logging class can be used with
    the root debugger.  ie. logging.info('example')
    """

    # create logger
    logger = logging.getLogger('root')
    logger.setLevel(settings.LOG_LEVEL)

    handler = logging.handlers.RotatingFileHandler(
             filename, 
             maxBytes    = settings.LOG_SIZE, 
             backupCount = settings.LOG_BACKUP)
    handler.setLevel(settings.LOG_LEVEL)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger