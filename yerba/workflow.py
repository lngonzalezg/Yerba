import heapq
import logging
import os

import core
import utils
from services import Service
logger = logging.getLogger('yerba.workflow')

def _format_args(args):
    argstring = ""

    for (arg, value, shorten) in args:
        val = str(value)

        if shorten == 1 and os.path.isabs(val):
            val = os.path.basename(val)

        argstring = ("%s %s %s" % (argstring, arg, val))

    return argstring

class Job(object):
    def __init__(self, cmd, script, arguments, description=''):
        self.cmd = cmd
        self.script = script
        self.args = arguments
        self.inputs = []
        self.outputs = []
        self._status = 'waiting'
        self._description = description
        self._info = {}
        self._errors = []
        self._options = {
            "accepted-return-codes" : [ 0 ],
            "allow-zero-length" : True,
            "retries" : 0
        }

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, options):
        """
        Updates the options to be used by the job
        """
        self._options = utils.ChainMap(options, self._options)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        logger.info('JOB: the status has been changed to %s', value)
        self._status = value

    @property
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, error):
        self._errors.append(error)

    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, info):
        logger.info("JOB (status: %s): The info field has been updated",
                self.status)
        self._info = info

    @property
    def description(self):
        return self._description

    @property
    def state(self):
        #FIXME add support for errors

        status = [
            ['status', self.status],
            ['description', self.description],
            ['errors', self.errors]
        ]

        status.extend(self.info.items())

        return dict(status)


    def clear(self):
        for output in self.outputs:
            with utils.ignored(OSError):
                os.remove(output)

    def running(self):
        return self._status == 'running'

    def completed(self, returned=None):
        '''Returns whether or not the job was completed.'''
        codes = self.options['accepted-return-codes']

        # No outputs present
        if not self.outputs:
            if self.info:
                returned = self.info['returned']

            return any(returned == code for code in codes)

        for fp in self.outputs:
            if isinstance(fp, list) and fp[1]:
                val = os.path.abspath(str(fp[0]))

                if not os.path.isdir(val):
                    return False

            elif self.options["allow-zero-length"]:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path):
                    return False
            else:
                path = os.path.abspath(str(fp))
                if not os.path.isfile(path) or utils.is_empty(path):
                    return False

        return True

    def ready(self):
        '''Returns that the job has its input files and is ready.'''
        for fp in self.inputs:
            if isinstance(fp, list) and fp[1]:
                val = os.path.abspath(str(fp[0]))

                if not os.path.isdir(val):
                    return False
            elif self.options["allow-zero-length"]:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path):
                    return False
            else:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path) or utils.is_empty(path):
                    return False

        return True

    def restart(self):
        self.options['retries'] = self.options['retries'] - 1

    def failed(self):
        return self.options['retries'] < 0

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

    def running(self):
        '''
        Return the set of jobs that are running
        '''
        return {job for job in self.workflow.jobs if job.running()}

    def failed(self):
        '''
        Return the set of jobs that failed
        '''
        return {job for job in self.workflow.jobs if job.failed()}

    def add_job_info(self, selected, info):
        '''
        Adds job information for the selected job
        '''
        for job in self._workflow.jobs:
            if job == selected:
                logger.info("WORKFLOW %s: Added info to job %s",
                        self.workflow.name, job)
                job.info = info

    def message(self):
        message = ("name: {0}, completed: {1}, failed: {2}, running: {3},",
        " waiting: {4}")

        jobs = (self.workflow.name,
            len(self.completed()),
            len(self.failed()),
            len(self.running()),
            len(self.waiting()))

        return "".join(message).format(*jobs)


    def log(self, filename):
        '''
        Logs the results of workflow.
        '''
        if self.workflow._logged or not self.workflow.log:
            return

        self._workflow._logged = True

        log_file = os.path.join(self.workflow.log, filename)

        with open(log_file, 'a') as fp:
            for job in self._workflow.jobs:
                fp.write('#' * 25 + '\n')
                if job.status == 'skipped':
                    fp.write('{0}\n'.format(job.description))
                    fp.write("Job: %s\n" % str(job))
                    fp.write("Skipped: The analysis was previously generated.\n")
                elif job.info:
                    msg = ("Job: {cmd}\n"
                        "Submitted at: {started}\n"
                        "Completed at: {ended}\n"
                        "Execution time: {elapsed} sec\n"
                        "Assigned to task: {taskid}\n"
                        "Return status: {returned}\n"
                        "Expected outputs: {outputs}\n"
                        "Command Output:\n{output}")
                    fp.write('{0}\n'.format(job.description))

                    outputs = []

                    for item in job.outputs:
                        if isinstance(item, list) and item[1]:
                            outputs.append(item[0])
                        else:
                            outputs.append(item)

                    job.info['outputs'] = ', '.join(outputs)

                    fp.write(msg.format(**job.info))
                else:
                    fp.write('{0}\n'.format(job.description))
                    fp.write("Job: %s\n" % str(job))
                    fp.write("The job was not run.\n")
                fp.write('#' * 25 + '\n\n')

    def status(self):
        '''
        Return the status of the workflow
        '''
        if (any(job.failed() for job in self._workflow.jobs) or
            any(job.status == 'failed' for job in self._workflow.jobs)):
            status = core.Status.Failed
        elif any(job.status == 'cancelled' for job in self._workflow.jobs):
            status = core.Status.Cancelled
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
        self._logged = False

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

def filter_options(options):
    """
    Returns the set of filtered options that are specified
    """
    return {key : value for (key, value) in options.iteritems()
                if value is not None}

def generate_workflow(pyobject):
    '''Generates a workflow from a python object.'''
    logger.info("######### Generate Workflow  ##########")
    job_objects = pyobject.get('jobs', [])

    if not job_objects:
        raise WorkflowError("The workflow does not contain any jobs.")

    name = pyobject.get('name', 'unnamed')
    level = pyobject.get('priority', 0)
    logpath = pyobject.get('logpath', None)
    jobs = [generate_job(job_object) for job_object in job_objects]

    workflow = Workflow(name, jobs, log=logpath, priority=level)
    logger.info("WORKFLOW %s has been generated.", name)
    logger.info("######### END Generate Workflow  ##########")

    return workflow

def generate_job(job_object):
    """
    Returns a job generated from a python object
    """
    (cmd, script, args) = (job_object['cmd'], job_object['script'], job_object['args'])

    if not cmd:
        raise JobError("The command name is NoneType.")

    if not args:
        raise JobError("The arguments are NoneType.")

    # Set the job_object description
    desc = job_object.get('description', '')
    new_job = Job(cmd, script, args, description=desc)
    logger.debug("Creating job %s",  new_job.description)

    # Set the job_object options
    options = job_object.get('options', {})
    logger.info("Additional job options being set %s", options)
    new_job.options = filter_options(options)

    # Add inputs
    inputs = job_object.get('inputs', []) or []
    if any(item is None for item  in inputs):
        raise JobError("The job has a NoneType input")

    new_job.inputs.extend(sorted(inputs))

    # Add outputs
    outputs = job_object.get('outputs', []) or []
    if any(fp is None for fp in outputs):
        raise JobError("The job has a NoneType output")

    new_job.outputs.extend(sorted(outputs))

    if 'overwrite' in job_object and int(job_object['overwrite']):
        logger.debug(("The job will overwrite previous"
            "results:\n%s"), new_job)
        new_job.clear()

    return new_job

class JobError(ValueError):
    pass

class WorkflowError(ValueError):
    pass
