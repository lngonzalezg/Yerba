from logging import getLogger
import itertools
import os

from work_queue import (Task, WorkQueue, WORK_QUEUE_DEFAULT_PORT)

logger = getLogger('yerba.services')

class Service(object):
    def initialize(self):
        '''Initializes the service'''

    def update(self):
        '''Update callback performed by the service.'''

    def stop(self):
        '''Stops the service'''

class WorkQueueService(Service):
    name = "workqueue"
    group = "scheduler"
    port = WORK_QUEUE_DEFAULT_PORT

    def initialize(self):
        '''Initializes work_queue for scheduling workers.'''
        self.tasks = {}
        self.counter = itertools.count()

        try:
            self.work_queue = WorkQueue(self.port, "coge", catalog=True)
            logger.info("Started work queue master on port %d", self.port)
        except:
            raise InitializeServiceException('Unable to start the work_queue')

    def stop(self):
        '''Removes all jobs from the queue and stops the work queue.'''
        self.cancel(self.tasks.keys())
        self.work_queue.shutdown_workers(0)

    def schedule(self, iterable, name, priority=None):
        '''Schedules jobs into work_queue'''
        for job in iterable:
            if not job.ready():
                continue

            cmd = str(job)
            task = Task(cmd)

            for input_file in job.inputs:
                remote_input = os.path.basename(input_file)
                task.specify_input_file(str(input_file), str(remote_input))

            for output_file in job.inputs:
                remote_output = os.path.basename(output_file)
                task.specify_output_file(str(output_file), str(remote_output),
                                         cache=False)

            new_id = self.work_queue.submit(task)
            logger.info("Scheduled job")
            self.tasks[new_id] = job

    def update(self):
        '''Updates workers being completed.'''
        task = self.work_queue.wait(0)

        if task:
            job = self.tasks[task.id]
        else:
            job = None

        iterable = WorkflowManager.fetchjobs(name)

        if job and job.completed():
            (priority, jobs) = self.workflows[name]
            del self.tasks[task.id]

            if len(jobs):
                ids = self.schedule(jobs.pop(0), name, priority)

    def info(self, task):
        '''Parses task into json object.'''
        pass

    def cancel(self, name):
        '''Removes the jobs based on there job id task id from the queue.'''
        for task in self.workflow[name]:
            self.work_queue.cancel_by_taskid(task.id)

        del self.workflow[name]

class Status(object):
    NotFound = -1
    Waiting = 0
    Running = 1
    Completed = 2
    Failed = 3
    Terminated = 4
    Started = 5
    Scheduled = 6
    Attached = 7
    Error = 8

class InitializeServiceException(Exception):
    """Exception raised when a service fails to initialize properly."""
    pass
