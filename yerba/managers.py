from logging import getLogger
from functools import wraps

import core
import utils
from workflow import generate_workflow, WorkflowHelper

logger = getLogger('yerba.manager')
SEPERATOR = '.'

class ServiceManager(object):
    core = {}
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

        if key in cls.core:
            logging.warn("This service already exists.")
        else:
            cls.core[key] = service

    @classmethod
    def deregister(cls, service):
        """Deregisters a service with the manager."""
        key = SEPERATOR.join((service.group, service.name))

        if key in cls.core:
            del core[key]

    @classmethod
    def get(cls, name, group):
        key = SEPERATOR.join((group, name))

        if key in cls.core:
            return cls.core[key]
        else:
            return None

    @classmethod
    def start(cls):
        """Starts the service manager """
        for service in cls.core.values():
            service.initialize()

        cls.RUNNING = True

    @classmethod
    def update(cls):
        '''Run service update callback.'''
        for service in cls.core.values():
            service.update()

    @classmethod
    def stop(cls):
        '''Stops the service manager and all core'''
        for service in cls.core.values():
            service.stop()

        cls.RUNNING = False

class WorkflowManager(object):
    workflows = {}

    @classmethod
    def submit(cls, json):
        '''Submits workflows to be scheduled'''
        try:
            (id, workflow) = generate_workflow(json)
        except Exception:
            logger.exception("The workflow could not be generated.")
            return core.Status.Error

        cls.workflows[id] = workflow
        items = []

        for job in workflow.jobs:
            if job.completed():
                job.status = 'skipped'
            elif job.ready():
                job.status = 'running'
                items.append(job)
            else:
                job.status = 'scheduled'

        scheduler = ServiceManager.get("workqueue", "scheduler")
        scheduler.schedule(items, id, workflow.log, priority=workflow.priority)

        return core.Status.Scheduled

    @classmethod
    def fetch(cls, id):
        '''Gets the next set of jobs to be run.'''
        iterable = []

        with utils.ignored(KeyError):
            workflow_helper = WorkflowHelper(cls.workflows[id])
            iterable = workflow_helper.waiting()

            for job in iterable:
                job.status = 'running'

        return iterable

    @classmethod
    def update(cls, id, job, info):
        '''Updates the job with details'''
        with utils.ignored(KeyError):
            workflow_helper = WorkflowHelper(cls.workflows[id])
            workflow_helper.add_job_info(job, info)

    @classmethod
    def status(cls, id):
        '''Gets the status of the current workflow.'''
        status = core.Status.NotFound
        data = []

        with utils.ignored(KeyError):
            workflow_helper = WorkflowHelper(cls.workflows[id])

            for job in workflow_helper.workflow.jobs:
                if job.status == 'running' and job.completed():
                    job.status = 'completed'
                elif job.failed():
                    job.status = 'failed'

                data.append({
                    'status' : job.status,
                    'description' : job.description,
                })

            status = workflow_helper.status()

        if status != core.Status.Running:
            workflow_helper.log()
            cls.workflows[id]._logged = True

        return (status, data)

    @classmethod
    def cancel(cls, id):
        '''Cancel the workflow from being run.'''
        status = core.Status.NotFound

        with utils.ignored(KeyError):
            scheduler = ServiceManager.get("workqueue", "scheduler")
            scheduler.cancel(id)
            workflow = cls.workflows[id]

            for job in workflow.jobs:
                if job.status == 'waiting':
                    job.status = 'cancelled'
                elif job.status == 'running':
                    job.status = 'cancelled'
                elif job.status == 'scheduled':
                    job.status = 'cancelled'

            status = core.Status.Cancelled

        return status
