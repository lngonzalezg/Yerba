from datetime import datetime
from logging import getLogger
from os import getloadavg
from time import time, sleep

from yerba.core import Status
from yerba.db import Database, WorkflowStore
from yerba.workflow import (generate_workflow, WorkflowHelper, WorkflowError,
                            log_not_run_job, log_job_info, log_skipped_job)
from yerba.utils import ignored, meminfo

logger = getLogger('yerba.manager')

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

        key = '.'.join((service.group, service.name))

        if key in cls.core:
            logger.warn("This service already exists.")
        else:
            cls.core[key] = service

    @classmethod
    def deregister(cls, service):
        """Deregisters a service with the manager."""
        key = '.'.join((service.group, service.name))

        if key in cls.core:
            del cls.core[key]

    @classmethod
    def get(cls, name, group):
        key = '.'.join((group, name))

        if key in cls.core:
            return cls.core[key]
        else:
            return None

    @classmethod
    def start(cls):
        """Starts the service manager """
        for service in cls.core.values():
            service.initialize()

        cls.refresh = 300
        cls.previous = time()
        cls.RUNNING = True

    @classmethod
    def update(cls):
        '''Run service update callback.'''
        for service in cls.core.values():
            service.update()

        current_time = time()
        if (current_time - cls.previous) > cls.refresh:
            cls.previous = current_time
            report_state()

    @classmethod
    def stop(cls):
        '''Stops the service manager and all core'''
        for service in cls.core.values():
            service.stop()

        cls.RUNNING = False

def report_state():
    wq = ServiceManager.get("workqueue", "scheduler")

    workflow_status = ["INTERNAL STATE CHECK\n----WORKFLOW STATUS--\n"]
    for (workflow_id, workflow) in WorkflowManager.workflows.items():
        helper = WorkflowHelper(workflow)
        msg = "--WORKFLOW (status = {0}): {1} ({2})\n"
        workflow_status.append(msg.format(helper.status(), workflow_id, helper.message()))


    stats = wq.queue.stats

    if len(workflow_status) < 2:
        workflow_status.append("--No workflows found\n")

    mem = meminfo()


    msg = ("\n---- System Info---\n"
    "--CPU LOAD: {} {} {}\n"
    "--Total Memory Avaible {}\n"
    "--Memory Free {}\n"
    "---- Work Queue Info --\n"
    "--The work queue started on {}\n"
    "--The total time sending data to workers is {}\n"
    "--The total time spent recieving data from the workers is {}\n"
    "--The total number of MB sent is {}\n"
    "--The total number of MB recieved is {}\n"
    "--The total number of workers joined {}\n"
    "--The total number of workers removed is {}\n"
    "--The total number of tasks completed is {}\n"
    "--The total number of tasks dispatched is {}\n"
    "----- Task info --\n"
    "--There are {} tasks waiting\n"
    "--There are {} tasks completed\n"
    "--There are {} tasks running\n"
    "---- Worker info --\n"
    "--There are {} workers initializing\n"
    "--There are {} workers ready\n"
    "--There are {} workers busy\n"
    "--There are {} workers full\n")

    dateformat="%d/%m/%y at %I:%M:%S%p"

    MICROSEC_PER_SECOND = 1000000.0
    BYTES_PER_MEGABYTE = 1048576.0

    dt1 = datetime.fromtimestamp(stats.start_time/ MICROSEC_PER_SECOND)
    start_time = dt1.strftime(dateformat)

    try:
        load1, load5, load15 = getloadavg()
    except:
        load1, load5, load15 = 0, 0, 0

    workqueue_status = msg.format(
        load1,
        load5,
        load15,
        mem["MemTotal"],
        mem["MemFree"],
        start_time,
        stats.total_send_time,
        stats.total_receive_time,
        stats.total_bytes_sent / BYTES_PER_MEGABYTE,
        stats.total_bytes_received / BYTES_PER_MEGABYTE,
        stats.total_workers_joined,
        stats.total_workers_removed,
        stats.total_tasks_complete,
        stats.total_tasks_dispatched,
        stats.tasks_waiting,
        stats.tasks_complete,
        stats.tasks_running,
        stats.workers_init,
        stats.workers_ready,
        stats.workers_busy,
        stats.workers_full)

    status = "".join([workqueue_status, "".join(workflow_status)])
    logger.debug(status)
    sleep(1)


class WorkflowManager(object):
    database = Database()
    store = None
    workflows = {}

    @classmethod
    def connect(cls, filename):
        '''Connect to workflow database'''
        cls.database.connect(filename)
        cls.store = WorkflowStore(cls.database)

    @classmethod
    def create(cls, workflow=None, jobs_object=None, status=Status.Initialized):
        '''Adds a new workflow to the database'''

        if workflow and jobs_object:
            workflow_id = cls.store.add_workflow(
                name=workflow.name, log=workflow.log, jobs=jobs_object,
                status=status, priority=workflow.priority)
        else:
            workflow_id = cls.store.add_workflow(status=status)

        logger.info("Generating new workflow")
        return (workflow_id, status)

    @classmethod
    def submit(cls, data):
        '''Generate and schedule the workflow to be run'''
        workflow_id = data.get('id', None)

        try:
            workflow = generate_workflow(data)
            logger.debug("WORKFLOW %s: submitted", workflow.name)
        except WorkflowError:
            logger.exception("WORKFLOW: the workflow failed to be generated")
        except Exception:
            logger.exception("""WORKFLOW: An unexpected error occured during
                            workflow generation""")
            return (None, Status.Error, None)

        # Check if the id was given otherwise try to find the workflow
        # Create a new entry if the workflow was not found
        # This allows a workflow to be unique and reusable
        if workflow_id:
            workflow_found = cls.store.get_workflow(workflow_id)
        else:
            workflow_found = cls.store.find_workflow(data['jobs'])

        if workflow_found:
            (workflow_id, _, _, _, _, _, _, status) = workflow_found

            if workflow_id in cls.workflows and status == Status.Running:
                return (workflow_id, status, None)

            if status == Status.Initialized:
                logger.info("Updating existing workflow")
                cls.store.update_workflow(workflow_id,
                    name=workflow.name, log=workflow.log, jobs=data['jobs'],
                    priority=workflow.priority)
        else:

            (workflow_id, _) = cls.create(workflow=workflow,
                                          jobs_object=data['jobs'])

        cls.workflows[workflow_id] = workflow
        scheduled_status = cls.schedule(workflow_id, workflow)

        return (workflow_id, scheduled_status, None)

    @classmethod
    def schedule(cls, workflow_id, workflow):
        '''Schedules a workflow by its id'''
        status = Status.Scheduled
        items  = []
        cls.store.update_status(workflow_id, status)

        for job in workflow.jobs:
            if job.completed():
                job.status = 'skipped'
                log_skipped_job(workflow.log, job)
            elif job.ready():
                job.status = 'running'
                items.append(job)
            else:
                job.status = 'scheduled'

        scheduler = ServiceManager.get("workqueue", "scheduler")
        scheduler.schedule(items, workflow_id, priority=workflow.priority)
        logger.info("WORKFLOW ID: %s", workflow_id)

        return status

    @classmethod
    def get_workflows(cls, ids):
        '''Returns all matching workflows in the job engine'''
        return cls.store.fetch(ids)

    @classmethod
    def fetch(cls, workflow_id):
        '''Gets the next set of jobs to be run.'''
        iterable = []

        with ignored(KeyError):
            workflow = cls.workflows[workflow_id]
            workflow_helper = WorkflowHelper(workflow)
            iterable = workflow_helper.waiting()

            for job in iterable:
                logger.debug('WORKFLOW %s: changing job %s status to running',
                        workflow.name, job)
                job.status = 'running'

        return iterable

    @classmethod
    def update(cls, workflow_id, job, info):
        '''Updates the job with details'''

        with ignored(KeyError):
            workflow = cls.workflows[workflow_id]
            workflow_helper = WorkflowHelper(workflow)
            workflow_helper.add_job_info(job, info)

            if workflow.log:
                log_job_info(workflow.log, job)

            if job.status == 'running' and job.completed():
                job.status = 'completed'
            elif job.failed():
                job.status = 'failed'

            status = workflow_helper.status()

            if status == Status.Failed:
                for job in workflow.jobs:
                    if job.status == 'scheduled':
                        log_not_run_job(workflow.log, job)

            if status != Status.Running:
                cls.store.update_status(workflow_id, status,
                                 completed=True)
                cls.workflows[workflow_id]._logged = True
            else:
                cls.store.update_status(workflow_id, status)

    @classmethod
    def status(cls, workflow_id):
        '''Gets the status of the current workflow.'''
        status = cls.store.get_status(workflow_id)
        jobs = []

        with ignored(KeyError):
            jobs = [job.state for job in cls.workflows[int(workflow_id)].jobs]

        return (status, jobs)

    @classmethod
    def cancel(cls, workflow_id):
        '''Cancel the workflow from being run.'''
        status = Status.NotFound

        with ignored(KeyError):
            workflow = cls.workflows[int(workflow_id)]
            logger.info(('WORKQUEUE %s: the workflow has been requested'
            'to be cancelled'), workflow.name)

            cls.store.update_status(workflow_id, Status.Cancelled)

            scheduler = ServiceManager.get("workqueue", "scheduler")
            scheduler.cancel(workflow_id)

            for job in workflow.jobs:
                if job.status == 'waiting':
                    job.status = 'cancelled'
                elif job.status == 'running':
                    job.status = 'cancelled'
                elif job.status == 'scheduled':
                    job.status = 'cancelled'

            status = Status.Cancelled

        return status
