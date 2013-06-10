from logging import getLogger

logger = getLogger('yerba.services')

class Service(object):
    def initialize(self):
        '''Initializes the service'''

    def update(self):
        '''Update callback performed by the service.'''

    def stop(self):
        '''Stops the service'''


class Status(object):
    NotFound = -1
    Waiting = 0
    Running = 1
    Completed = 2
    Failed = 3
    Terminated = 4
    Started = 5
    Scheduled = 6
    Attached = 7
    Error = 8

class InitializeServiceException(Exception):
    """Exception raised when a service fails to initialize properly."""
    pass
