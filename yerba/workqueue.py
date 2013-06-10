import itertools
import os
from logging import getLogger

from work_queue import (Task, WorkQueue, WORK_QUEUE_DEFAULT_PORT,
                        WORK_QUEUE_INPUT, WORK_QUEUE_OUTPUT, set_debug_flag,
                        WORK_QUEUE_MASTER_MODE_CATALOG)
import services
from managers import WorkflowManager

logger = getLogger('yerba.workqueue')


def write_to_log(logfile, task):
    with open(str(logfile), 'a') as fp:
        fp.write("Job: %s\n" % task.command)
        fp.write("Submitted at: %d\n" % task.submit_time)
        fp.write("Execution time of command: %d\n" % task.cmd_execution_time)
        fp.write("Completed at: %d\n" % task.finish_time)
        fp.write("Assigned to task: %d\n" % task.id)
        fp.write("Return status: %d\n" % task.return_status)
        fp.write("{task}\n".format(task=task.output))

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
            self.queue = WorkQueue(name="coge-test", catalog=True)
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
        for job in iterable:
            if not job.ready():
                continue

            cmd = str(job)
            task = Task(cmd)

            for input_file in job.inputs:
                remote_input = os.path.basename(input_file)
                task.specify_input_file(str(input_file), str(remote_input),
                                       WORK_QUEUE_INPUT)

            for output_file in job.outputs:
                remote_output = os.path.basename(output_file)
                task.specify_file(str(output_file), str(remote_output),
                                  WORK_QUEUE_OUTPUT, cache=False)

            new_id = self.queue.submit(task)

            logger.info("Scheduled job with id: %s", new_id)
            self.tasks[new_id] = (name, log, job)

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
            write_to_log(log, task)
            iterable = WorkflowManager.fetch(name)

            if job.completed():
                del self.tasks[task.id]
            elif not job.failed():
                job.restart()
                iterable.append(job)
            else:
                del self.tasks[task.id]

            self.schedule(iterable, name, log)

    def cancel(self, name):
        '''
        Removes the jobs based on there job id task id from the queue.
        '''
        for task in self.workflow[name]:
            self.queue.cancel_by_taskid(task.id)

        del self.workflow[name]
