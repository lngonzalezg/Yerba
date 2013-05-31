from logging import getLogger
from functools import wraps

from service import (Service, Status)
import workflow

logger = getLogger('yerba.manager')
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
            service.initialize()

        cls.RUNNING = True

    @classmethod
    def update(cls):
        '''Run service update callback.'''
        (srv.update() for (srv_group, srv) in cls.services)

    @classmethod
    def stop(cls):
        '''Stops the service manager and all services'''
        (srv.stop() for (srv_group, srv) in cls.services)
        cls.RUNNING = False

def route(request):
    def callback(f):
        Router.add(request, f)
    return callback

REQUEST = 'request'
DATA = 'data'

class Router(object):
    routes = {}

    @classmethod
    def add(cls, route, fnc):
        '''Adds a new route to the dispatch manager'''
        if route in cls.routes:
            logger.warn("The route %s already exists.", route)
        else:
            cls.routes[route] = fnc

    @classmethod
    def remove(cls, route):
        '''Removes a route to the dispatch manager'''
        if route in cls.routes:
            del cls.routes[route]
        else:
            raise DispatchRouteNotFoundException("Route not found")

    @classmethod
    def dispatch(cls, request):
        '''Dispatches request to perform specified task'''
        if not request and REQUEST not in request or DATA not in request:
            raise RequestError("Invalid request")

        route = request[REQUEST]
        data = request[DATA]

        if route in cls.routes:
            return cls.routes[route](data)
        else:
            raise DispatchRouteNotFoundException("Route %s not found", route)

class WorkflowManager(object):
    workflows = {}

    @classmethod
    def submit(cls, json):
        '''Submits workflows to be scheduled'''
        (name, priority, log, jobs) = workflow.generate_workflow(json)

        if name in cls.workflows:
            return Status.Attached

        items = [job for job in jobs if job.ready() and not job.completed()]
        scheduler = ServiceManager.get("workqueue", "scheduler")
        scheduler.schedule(items, name)

        cls.workflows[name] = (priority, log, jobs)
        return Status.Scheduled

    @classmethod
    def fetch(cls, id):
        '''Gets the next set of jobs to be run.'''
        if id not in cls.workflows:
            return

        (priority, log, jobs) = cls.workflows[id]

        return [job for job in jobs if job.ready() and not job.completed()]

    @classmethod
    def status(cls, id):
        '''Gets the status of the current workflow.'''
        if id not in cls.workflows:
            return Status.NotFound

        (priority, log, jobs) = cls.workflows[id]

        finished = bool([job for job in jobs if not job.completed()])

        if not finished:
            return Status.Completed
        else:
            return Status.Running

    @classmethod
    def cancel(cls, id):
        '''Cancel the workflow from being run.'''
        try:
            del cls.workflows[id]
            status = Status.Terminated
        except KeyError as e:
            status = Status.NotFound

        return status

class RequestError(Exception):
    '''Raised when a request is invalid.'''

class DispatchRouteNotFoundException(Exception):
    '''Exception raised when a dispatch route is not found.'''
