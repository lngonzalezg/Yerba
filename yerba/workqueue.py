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
PROJECT_NAME = "yerba"

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

    @classmethod
    def set_project(cls, name):
        cls.project_name = name

    def initialize(self):
        '''
        Initializes work_queue for scheduling workers.

        A new work_queue will be created if an existing queue is not connected
        on the port. If an existing queue is running then an
        InitializeServiceException will be raised.
        '''
        self.tasks = {}

        try:
            self.queue = WorkQueue(name=self.project_name, catalog=True)
            logger.info("Started work queue master on port %d", self.port)
        except Exception:
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

            skip = False

            for (taskid, item) in self.tasks.items():
                (names, log, job) = item
                if new_job == job:
                    if name not in names:
                        names.append(name)

                    self.tasks[taskid] = (names, log, job)
                    logger.info("The job is already running skipping.")
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

            logger.info("Scheduled job with id: %s", new_id)
            self.tasks[new_id] = ([name], log, new_job)

    def update(self):
        '''
        Updates the scheduled workflow.

        If a task is completed new tasks from the workflow will be scheduled.
        '''

        while not self.queue.empty():
            task = self.queue.wait(0)

            if not task:
                break

            logger.info("Task returned: %d", task.return_status)
            logger.info("Fetching new jobs to be run.")
            (names, log, job) = self.tasks[task.id]
            job.task_info = task
            items = {}

            if job.completed():
                for name in names:
                    iterable = managers.WorkflowManager.fetch(name)
                    if iterable:
                        items[name] = iterable

                del self.tasks[task.id]
                write_to_log(log, task)
            elif not job.failed():
                job.restart()
                for name in names:
                    items[name] = [job]
                del self.tasks[task.id]
            else:
                write_to_log(log, task)
                del self.tasks[task.id]

            for (name, iterable) in items.items():
                self.schedule(iterable, name, log)

    def cancel(self, name):
        '''
        Removes the jobs based on there job id task id from the queue.
        '''
        for (taskid, item) in self.tasks.items():
            (names, log, job) = item

            if name in names:
                names.remove(name)
            else:
                continue

            if not names:
                del self.tasks[taskid]
                self.queue.cancel_by_taskid(taskid)
            else:
                self.tasks = (names, log, job)

