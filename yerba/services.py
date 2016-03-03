# -*- coding: utf-8 -*-
from logging import getLogger

logger = getLogger('yerba.services')

class Service(object):
    def initialize(self):
        '''Initializes the service'''

    def update(self):
        '''Update callback performed by the service.'''

    def stop(self):
        '''Stops the service'''

class InitializeServiceException(Exception):
    """Exception raised when a service fails to initialize properly."""
    pass
