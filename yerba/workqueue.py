from __future__ import division
import datetime
import itertools
import logging
import sys
import os

from work_queue import (Task, WorkQueue, WORK_QUEUE_RANDOM_PORT,
                        WORK_QUEUE_INPUT, WORK_QUEUE_OUTPUT, set_debug_flag,
                        WORK_QUEUE_MASTER_MODE_CATALOG)
import services
import managers

logger = logging.getLogger('yerba.workqueue')

name = "yerba"

def get_task_info(task):
    dateformat="%d/%m/%y at %I:%M:%S%p"
    DIV = 1000000.0

    dt1 = datetime.datetime.fromtimestamp(task.submit_time / DIV)
    start_time = dt1.strftime(dateformat)

    dt2 = datetime.datetime.fromtimestamp(task.finish_time / DIV)
    finish_time = dt2.strftime(dateformat)
    execution_time = task.cmd_execution_time / DIV

    return {
        'cmd' : task.command,
        'started' : start_time,
        'ended' : finish_time,
        'elapsed' : execution_time,
        'taskid' : task.id,
        'returned' : task.return_status,
        'output' : task.output,
    }

class WorkQueueService(services.Service):
    name = "workqueue"
    group = "scheduler"

    def __init__(self, config):
        self.tasks = {}

        try:
            self.project = config['project']
            self.catalog_server = config['catalog_server']
            self.catalog_port = int(config['catalog_port'])
            self.port = int(config['port'])
            self.log = config['log']

            if config['debug']:
                set_debug_flag('debug')
        except KeyError:
            logging.exception("Invalid workqueue configuration")
            sys.exit(1)

    def initialize(self):
        '''
        Initializes work_queue for scheduling workers.

        A new work_queue will be created if an existing queue is not connected
        on the port. If an existing queue is running then an
        InitializeServiceException will be raised.
        '''
        try:
            self.queue = WorkQueue(name=self.project, catalog=True, port=-1)
            self.queue.specify_catalog_server(self.catalog_server,
                    self.catalog_port)
            self.queue.specify_log(self.log)

            logger.info('WORKQUEUE %s: Starting work queue on port %s',
                    self.project, self.queue.port)
        except Exception:
            logging.exception("The work queue could not be started")
            sys.exit(1)

    def stop(self):
        '''
        Removes all jobs from the queue and stops the work queue.
        '''
        logger.info('WORKQUEUE %s: Stopping work queue on port %s',
                self.project, self.queue.port)
        self.queue.shutdown_workers(0)

    def schedule(self, iterable, name, priority=None):
        '''
        Schedules jobs into work_queue
        '''
        logger.info("######### WORKQUEUE SCHEDULING ##########")
        for new_job in iterable:
            logger.info('WORKQUEUE %s: The workflow %s is scheduling job %s', self.project, name, new_job)

            if not new_job.ready():
                logger.info('WORKFLOW %s: Job %s was not scheduled waiting on inputs', name, new_job)
                continue

            skip = False

            for (taskid, item) in self.tasks.items():
                (names, job) = item
                if new_job == job:
                    if name not in names:
                        names.append(name)

                    logger.info(('WORKQUEUE %s: This job has already been'
                        'assigned to task %s'), self.project, taskid)

                    self.tasks[taskid] = (names, job)
                    skip = True
                    break

            if skip:
                continue

            cmd = str(new_job)
            task = Task(cmd)

            for input_file in new_job.inputs:
                remote_input = os.path.basename(input_file)
                task.specify_input_file(str(input_file), str(remote_input),
                                       WORK_QUEUE_INPUT)

            for output_file in new_job.outputs:
                remote_output = os.path.basename(output_file)
                task.specify_file(str(output_file), str(remote_output),
                                  WORK_QUEUE_OUTPUT, cache=False)

            new_id = self.queue.submit(task)

            logger.info('WORKQUEUE %s: Task has been submited and assigned [id %s]', self.project, new_id)

            self.tasks[new_id] = ([name], new_job)

        logger.info("######### WORKQUEUE END SCHEDULING ##########")

    def update(self):
        '''
        Updates the scheduled workflow.

        If a task is completed new tasks from the workflow will be scheduled.
        '''
        task = self.queue.wait(0)

        if not task:
            return

        logger.info("######### WORKQUEUE UPDATING ##########")
        logger.info("WORKQUEUE %s: Fetching task from the work queue",
                self.project)
        logger.debug(('WORKQUEUE %s: Recieved task %s from work_queue with'
            ' return_status %s'), self.project, task.id, task.return_status)

        if task.id not in self.tasks:
            logger.info(('WORKQUEUE %s: The job for id %s could ',
                'not be found.'), self.project, task.id)
            return

        (names, job) = self.tasks[task.id]
        info = get_task_info(task)

        items = {}

        if job.completed():
            for name in names:
                fetch_message = ('WORKQUEUE %s: Fetching next jobs'
                        'in workflow %s')
                logger.info(fetch_message, self.project, name)
                iterable = managers.WorkflowManager.fetch(name)
                if iterable:
                    items[name] = iterable
                managers.WorkflowManager.update(name, job, info)

            del self.tasks[task.id]

        elif not job.failed():
            logger.info(('WORKQUEUE %s: The task %s failed attempting'
                    'to rerun the task'), self.project, task.id)
            job.restart()
            for name in names:
                items[name] = [job]
                managers.WorkflowManager.update(name, job, info)

            del self.tasks[task.id]
        else:
            logger.info(('WORKQUEUE %s: The task %s failed notifying'
                'workflows %s'), self.project, task.id, ', '.join(names))
            for name in names:
                managers.WorkflowManager.update(name, job, info)

            del self.tasks[task.id]

        for (name, iterable) in items.items():
            self.schedule(iterable, name)

        logger.info("######### END WORKQUEUE UPDATING ##########")

    def cancel(self, name):
        '''
        Removes the jobs based on there job id task id from the queue.
        '''
        for (taskid, item) in self.tasks.items():
            (names, job) = item

            logger.info('WORKFLOW %s: Requesting task %s to be cancelled',
                    name, taskid)

            if name in names:
                names.remove(name)
            else:
                continue

            if not names:
                del self.tasks[taskid]
                logger.info('WORKQUEUE %s: The task %s was cancelled',
                    self.project, self.task.id)
                self.queue.cancel_by_taskid(taskid)
            else:
                msg = ('WORKQUEUE %s: The task %s was not cancelled '
                        'workflows %s depend on the task')
                logger.info(msg, self.project, taskid, ', '.join(names))
                self.tasks[taskid] = (names, job)

