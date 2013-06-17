from logging import getLogger
from functools import wraps

import services
import utils
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
        for service in cls.services.values():
            service.update()

    @classmethod
    def stop(cls):
        '''Stops the service manager and all services'''
        for service in cls.services.values():
            service.stop()

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
        try:
            (name, log, priority, jobs) = workflow.generate_workflow(json)
        except:
            logger.exception("The workflow could not be generated.")
            return services.Status.Error


        if name in cls.workflows:
            return services.Status.Attached

        items = []

        for job in jobs:
            if not job.completed():
                items.append(job)
            else:
                workflow.log_skipped_job(log, job)

        scheduler = ServiceManager.get("workqueue", "scheduler")
        scheduler.schedule(items, name, log)

        cls.workflows[name] = (priority, log, jobs)

        return services.Status.Scheduled

    @classmethod
    def fetch(cls, id):
        '''Gets the next set of jobs to be run.'''
        iterable = []
        with utils.ignored(KeyError):
            (priority, log, jobs) = cls.workflows[id]
            iterable = [job for job in jobs if job.ready() and not job.completed()]

        return iterable

    @classmethod
    def status(cls, id):
        '''Gets the status of the current workflow.'''
        status = services.Status.NotFound

        with utils.ignored(KeyError):
            (priority, log, jobs) = cls.workflows[id]
            jobs = [job for job in jobs if not job.completed()]

            if any(job.failed() for job in jobs):
                status = services.Status.Failed
                del cls.workflows[id]
            elif all(job.completed() for job in jobs):
                status = services.Status.Completed
                del cls.workflows[id]
            else:
                status = services.Status.Running

        return status

    @classmethod
    def cancel(cls, id):
        '''Cancel the workflow from being run.'''
        status = services.Status.NotFound

        with ignored(KeyError):
            del cls.workflows[id]
            status = services.Status.Terminated

        return status

class RequestError(Exception):
    '''Raised when a request is invalid.'''

class RouteNotFound(RequestError):
    '''Exception raised when a dispatch route is not found.'''
