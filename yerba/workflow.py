import heapq
import logging
import os

import core
import utils
from services import Service
logger = logging.getLogger('yerba.workflow')

def log_skipped_job(log, job):
    with open(log, 'a') as fp:
        fp.write('#' * 25 + '\n')
        fp.write("Job: %s\n" % job)
        fp.write("Skipped: The analysis was previously generated.\n")
        fp.write('#' * 25 + '\n\n')

def _format_args(args):
    argstring = ""

    for (arg, value, shorten) in args:
        if shorten == 1:
            val = os.path.basename(str(value))
        else:
            val = str(value)

        argstring = ("%s %s %s" % (argstring, arg, val))

    return argstring

class Job(object):
    def __init__(self, cmd, script, arguments, retries=3, description=''):
        self.cmd = cmd
        self.script = script
        self.args = arguments
        self.inputs = []
        self.outputs = []
        self.retries = retries
        self._status = 'waiting'
        self._description = description

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def description(self):
        return self._description

    def clear(self):
        for output in self.outputs:
            with utils.ignored(OSError):
                os.remove(output)

    def completed(self):
        '''Returns whether or not the job was completed.'''
        missing = [fp for fp in self.outputs if not os.path.isfile(fp)]
        return not bool(missing)

    def ready(self):
        '''Returns that the job has its input files and is ready.'''
        missing = [fp for fp in self.inputs if not os.path.isfile(fp)]
        return not bool(missing)

    def restart(self):
        self.retries = self.retries - 1

    def failed(self):
        return self.retries <= 0

    def __eq__(self, other):
        return (sorted(other.inputs) == sorted(self.inputs) and
                sorted(other.outputs) == sorted(self.outputs) and
                str(other) == str(self))

    def __repr__(self):
        return ' '.join([self.cmd, _format_args(self.args)])

    def __str__(self):
        return repr(self)

class WorkflowHelper(object):
    def __init__(self, workflow):
        self._workflow = workflow

    @property
    def workflow(self):
        return self._workflow

    def waiting(self):
        '''
        Return the set of jobs waiting to be scheduled.
        '''
        return self.ready() - self.completed()

    def completed(self):
        '''
        Return the set of jobs that are completed
        '''
        return {job for job in self._workflow.jobs if job.completed()}

    def ready(self):
        '''
        Return the set of jobs that are ready
        '''
        return {job for job in self._workflow.jobs if job.ready()}

    def status(self):
        '''
        Return the status of the workflow
        '''
        if (any(job.failed() for job in self._workflow.jobs) or
            any(job.status == 'failed' for job in self._workflow.jobs)):
            status = core.Status.Failed
        if any(job.status == 'cancelled' for job in self._workflow.jobs):
            status = core.Status.Terminated
        elif self.waiting():
            status = core.Status.Running
        else:
            status = core.Status.Completed

        return status

class Workflow(object):
    def __init__(self, name, jobs, log=None, priority=0):
        self._name = name
        self._log = log
        self._priority = priority
        self._jobs = jobs

    @property
    def jobs(self):
        return self._jobs

    @property
    def log(self):
        return self._log

    @property
    def name(self):
        return self._name

    @property
    def priority(self):
        return self._priority

def generate_workflow(pyobject):
    '''Generates a workflow from a python object.'''
    if 'name' not in pyobject or 'jobs' not in pyobject:
        raise WorkflowError("The workflow format was invalid.")

    if not len(pyobject['jobs']):
        raise WorkflowError("The workflow does not contain any jobs.")

    identity = pyobject['id']
    name = pyobject['name']
    jobs = []
    logfile = pyobject['logfile']

    for job in pyobject['jobs']:
        (cmd, script, args) = (job['cmd'], job['script'], job['args'])

        if 'description' in job:
            new_job = Job(cmd, script, args, description=job['description'])
        else:
            new_job = Job(cmd, script, args)

        if not cmd:
            raise JobError("The command name is NoneType.")
        if not args:
            raise JobError("The arguments are NoneType.")

        if 'inputs' in job and job['inputs']:
            if any(fp is None for fp in job['inputs']):
                raise JobError("Workflow %s has a NoneType input" % name)

            inputs = [os.path.abspath(str(item)) for item in job['inputs']]
            new_job.inputs.extend(sorted(inputs))

        if 'outputs' in job:
            if any(fp is None for fp in job['outputs']):
                raise JobError("Workflow %s has a NoneType output" % name)

            outputs = [os.path.abspath(str(item)) for item in job['outputs']]
            new_job.outputs.extend(sorted(outputs))

        if 'overwrite' in job and int(job['overwrite']):
            logger.info("The job will be restarted.")
            new_job.clear()

        jobs.append(new_job)

    if 'priority' in pyobject:
        workflow = Workflow(name, jobs, log=logfile, priority=priority)
    else:
        workflow = Workflow(name, jobs, log=logfile)

    return (identity, workflow)

class JobError(ValueError):
    pass

class WorkflowError(ValueError):
    pass
