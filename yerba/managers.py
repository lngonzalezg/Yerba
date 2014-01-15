from logging import getLogger
from functools import wraps
import datetime
import time
import os

import core
import utils
from workflow import generate_workflow, WorkflowHelper, WorkflowError, JobError

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

        cls.refresh = 300
        cls.previous = time.time()
        cls.RUNNING = True

    @classmethod
    def update(cls):
        '''Run service update callback.'''
        for service in cls.core.values():
            service.update()

        current_time = time.time()
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
    for (id, workflow) in WorkflowManager.workflows.items():
        helper = WorkflowHelper(workflow)
        msg = "--WORKFLOW (status = {0}): {1} ({2})\n"
        workflow_status.append(msg.format(helper.status(), id, helper.message()))


    stats = wq.queue.stats

    if len(workflow_status) < 2:
        workflow_status.append("--No workflows found\n")

    mem = utils.meminfo()


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

    dt1 = datetime.datetime.fromtimestamp(stats.start_time/ MICROSEC_PER_SECOND)
    start_time = dt1.strftime(dateformat)

    try:
        load1, load5, load15 = os.getloadavg()
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
    time.sleep(1)

class WorkflowManager(object):
    workflows = {}

    @classmethod
    def submit(cls, json):
        '''Submits workflows to be scheduled'''
        try:
            (id, workflow) = generate_workflow(json)
            logger.debug("WORKFLOW %s: submitted with id %s", workflow.name, id)
        except (JobError, WorkflowError):
            logger.exception("WORKFLOW: the workflow failed to be generated")
            return core.Status.Error
        except Exception:
            logger.exception("WORKFLOW: An unexpected error occured during workflow generation")
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
        scheduler.schedule(items, id, priority=workflow.priority)

        return core.Status.Scheduled

    @classmethod
    def fetch(cls, id):
        '''Gets the next set of jobs to be run.'''
        iterable = []

        with utils.ignored(KeyError):
            workflow = cls.workflows[id]
            workflow_helper = WorkflowHelper(workflow)
            iterable = workflow_helper.waiting()

            for job in iterable:
                logger.debug('WORKFLOW %s: changing job %s status to running',
                        workflow.name, job)
                job.status = 'running'

        return iterable

    @classmethod
    def update(cls, id, job, info):
        '''Updates the job with details'''
        with utils.ignored(KeyError):
            workflow = cls.workflows[id]
            workflow_helper = WorkflowHelper(workflow)
            workflow_helper.add_job_info(job, info)
            logger.info("WORKFLOW %s: updating the job %s",
                    workflow.name, job)

    @classmethod
    def status(cls, id):
        '''Gets the status of the current workflow.'''
        status = core.Status.NotFound
        data = []

        with utils.ignored(KeyError):
            workflow = cls.workflows[id]
            workflow_helper = WorkflowHelper(workflow)
            logger.debug("WORKFLOW %s: status check %s", workflow.name,
                    workflow_helper.message())

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
            workflow = cls.workflows[id]
            logger.info(('WORKQUEUE %s: the workflow has been requested'
            'to be cancelled'), workflow.name)

            scheduler = ServiceManager.get("workqueue", "scheduler")
            scheduler.cancel(id)

            for job in workflow.jobs:
                if job.status == 'waiting':
                    job.status = 'cancelled'
                elif job.status == 'running':
                    job.status = 'cancelled'
                elif job.status == 'scheduled':
                    job.status = 'cancelled'

            status = core.Status.Cancelled

        return status
