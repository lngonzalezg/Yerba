import logging

from service import (Service, StatusService)

logger = logging.getLogger('yerba.manager')

SEPERATOR = '.'

class ServiceManager(object):
    services = {}
    RUNNING = False

    @classmethod
    def running(cls):
        return cls.RUNNING

    @classmethod
    def register(cls, service):
        """ Registers a service with the manager."""
        if not hasattr(service, 'name'):
            logger.error("Service does not have a name.")
            return

        if not hasattr(service, 'group'):
            logger.error("Service does not belong to a group.")
            return

        key = SEPERATOR.join((service.group, service.name))

        if key in cls.services:
            logging.warn("This service already exists.")
        else:
            cls.services[key] = service

    @classmethod
    def deregister(cls, service):
        """Deregisters a service with the manager."""
        key = SEPERATOR.join((service.group, service.name))

        if key in cls.services:
            del services[key]

    @classmethod
    def get(cls, name, group):
        key = SEPERATOR.join((group, name))

        if key in cls.services:
            return cls.services[key]
        else:
            return None

    @classmethod
    def start(cls):
        """Starts the service manager """
        for service in cls.services.values():
            if isinstance(service, Service):
                service.initialize()

        cls.RUNNING = True

    @classmethod
    def stop(cls):
        """Stops the service manager """
        map(srv.stop() for (srv_group, srv) in services)
        cls.RUNNING = False

    @classmethod
    def initialize(cls, services=None):
        """Initializes default services to be used."""
        ServiceManager.register(StatusService())
