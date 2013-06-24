from __future__ import division
import itertools
import os
import datetime
import logging

from work_queue import (Task, WorkQueue, WORK_QUEUE_DEFAULT_PORT,
                        WORK_QUEUE_INPUT, WORK_QUEUE_OUTPUT, set_debug_flag,
                        WORK_QUEUE_MASTER_MODE_CATALOG)
import services
import managers

logger = logging.getLogger('yerba.workqueue')

DIV = 1000000.0
_dateformat="%d/%m/%y at %I:%M:%S%p"
PROJECT_NAME = "coge"

def write_to_log(logfile, task):
    with open(str(logfile), 'a') as fp:
        dt1 = datetime.datetime.fromtimestamp(task.submit_time / DIV)
        start_time = dt1.strftime(_dateformat)

        dt2 = datetime.datetime.fromtimestamp(task.finish_time / DIV)
        finish_time = dt2.strftime(_dateformat)
        execution_time = task.cmd_execution_time / DIV

        msg = ("Job: {command}\n"
            "Submitted at: {start_date}\n"
            "Completed at: {end_date}\n"
            "Execution time: {delta} sec\n"
            "Assigned to task: {id}\n"
            "Return status: {status}\n"
            "{task}\n")

        fp.write(msg.format(command=task.command,
            start_date=start_time,
            end_date=finish_time,
            delta=execution_time,
            id=task.id,
            status = task.return_status,
            task = task.output))

class WorkQueueService(services.Service):
    name = "workqueue"
    group = "scheduler"
    port = WORK_QUEUE_DEFAULT_PORT

    def initialize(self):
        '''
        Initializes work_queue for scheduling workers.

        A new work_queue will be created if an existing queue is not connected
        on the port. If an existing queue is running then an
        InitializeServiceException will be raised.
        '''
        self.tasks = {}
        self.counter = itertools.count()

        try:
            self.queue = WorkQueue(name=PROJECT, catalog=True)
            logger.info("Started work queue master on port %d", self.port)
        except:
            raise InitializeServiceException('Unable to start the work_queue')

    def stop(self):
        '''
        Removes all jobs from the queue and stops the work queue.
        '''
        self.queue.shutdown_workers(0)

    def schedule(self, iterable, name, log, priority=None):
        '''
        Schedules jobs into work_queue
        '''
        for new_job in iterable:
            if not new_job.ready():
                continue

            if any(new_job == job for (name, log, job) in self.tasks.values()):
                logger.info("The job is already running skipping.")
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

            logger.info("Scheduled job with id: %s", new_id)
            self.tasks[new_id] = (name, log, new_job)

    def update(self):
        '''
        Updates the scheduled workflow.

        If a task is completed new tasks from the workflow will be scheduled.
        '''
        if not self.queue.empty():
            task = self.queue.wait(0)
        else:
            task = None

        if task:
            logger.info("Task returned: %d", task.return_status)
            logger.info("Fetching new jobs to be run.")
            (name, log, job) = self.tasks[task.id]
            job.task_info = task
            iterable = []

            if job.completed():
                del self.tasks[task.id]
                write_to_log(log, task)
                iterable.extend(managers.WorkflowManager.fetch(name))
            elif not job.failed():
                job.restart()
                iterable.append(job)
            else:
                write_to_log(log, task)
                del self.tasks[task.id]

            self.schedule(iterable, name, log)

    def cancel(self, iterable):
        '''
        Removes the jobs based on there job id task id from the queue.
        '''
        for (task, job) in self.tasks:
            if any(job == item for item in iterable):
                self.queue.cancel_by_taskid(task.id)
                del self.tasks[task.id]
