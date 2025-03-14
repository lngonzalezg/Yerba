# -*- coding: utf-8 -*-
from __future__ import division

from datetime import datetime
from logging import getLogger
from os.path import abspath, basename
from sys import exit

import work_queue as wq

from yerba.core import TASK_DONE
from yerba.services import Service

logger = getLogger('yerba.workqueue')
name = "yerba"
MAX_OUTPUT = 65536

def get_task_info(task):
    dateformat="%d/%m/%y at %I:%M:%S%p"
    DIV = 1000000.0

    dt1 = datetime.fromtimestamp(task.submit_time / DIV)
    start_time = dt1.strftime(dateformat)

    dt2 = datetime.fromtimestamp(task.finish_time / DIV)
    finish_time = dt2.strftime(dateformat)
    execution_time = task.cmd_execution_time / DIV

    return {
        'cmd' : task.command,
        'started' : start_time,
        'ended' : finish_time,
        'elapsed' : execution_time,
        'taskid' : task.id,
        'returned' : task.return_status,
        'output' : repr(task.output[:MAX_OUTPUT]),
    }

class WorkQueueService(Service):
    name = "workqueue"
    group = "scheduler"

    def __init__(self, config, notifier):
        self.tasks = {}
        self.notifier = notifier

        try:
            self.project = config['project']
            self.catalog_server = config['catalog_server']
            self.catalog_port = int(config['catalog_port'])
            self.port = int(config['port'])
            self.log = config['log']

            if config['debug']:
                wq.set_debug_flag('all')
        except KeyError:
            logger.exception("Invalid workqueue configuration")
            exit(1)

    def initialize(self):
        '''
        Initializes work_queue for scheduling workers.

        A new work_queue will be created if an existing queue is not connected
        on the port. If an existing queue is running then the process will exit.
        '''
        try:
            self.queue = wq.WorkQueue(name=self.project, port=-1)
            self.queue.specify_catalog_server(self.catalog_server,
                    self.catalog_port)
            self.queue.specify_log(self.log)

            logger.info('WORKQUEUE %s: Starting work queue on port %s',
                    self.project, self.queue.port)
        except Exception:
            logger.exception("The work queue could not be started")
            exit(1)

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
            task = wq.Task(cmd)

            for input_file in new_job.inputs:
                if isinstance(input_file, list) and input_file[1]:
                    remote_input = basename(abspath(input_file[0]))
                    task.specify_directory(str(input_file[0]), str(remote_input),
                                    wq.WORK_QUEUE_INPUT, recursive=1)
                else:
                    remote_input = basename(abspath(input_file))
                    task.specify_input_file(str(input_file), str(remote_input),
                                    wq.WORK_QUEUE_INPUT)

            for output_file in new_job.outputs:
                if isinstance(output_file, list):
                    remote_output = basename(abspath(output_file[0]))
                    task.specify_directory(str(output_file[0]), str(remote_output),
                                    wq.WORK_QUEUE_OUTPUT, recursive=1, cache=False)
                else:
                    remote_output = basename(abspath(output_file))
                    task.specify_file(str(output_file), str(remote_output),
                                    wq.WORK_QUEUE_OUTPUT, cache=False)

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

        try:
            logger.debug("INSPECTING TASK: %s", str(task))
            logger.debug(('WORKQUEUE %s: Recieved task %s from work_queue with'
                ' return_status %s'), self.project, task.id, task.return_status)
        except:
            logger.debug("Couldn't inspect the task")

        if task.id not in self.tasks:
            logger.info(('WORKQUEUE %s: The job for id %s could ',
                'not be found.'), self.project, task.id)
            return

        (names, job) = self.tasks[task.id]
        info = get_task_info(task)
        del self.tasks[task.id]

        for workflow in names:
            self.notifier.notify(TASK_DONE, workflow, job, info)

        logger.info("######### WORKQUEUE END UPDATING ##########")

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
                task = self.queue.cancel_by_taskid(taskid)

                if task:
                    del self.tasks[taskid]
                    logger.info("WORKQUEUE %s: The task %s was cancelled",
                        self.project, task.taskid)
                else:
                    logger.error("WORKQUEUE %s: failed to cancel %s",
                        self.project, taskid)
            else:
                msg = ('WORKQUEUE %s: The task %s was not cancelled '
                        'workflows %s depend on the task')
                logger.info(msg, self.project, taskid, ', '.join(names))
                self.tasks[taskid] = (names, job)

