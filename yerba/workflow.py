from itertools import groupby
import logging
import os

from yerba import core
from yerba import db
from yerba import utils

logger = logging.getLogger('yerba.workflow')

WAITING = 'waiting'
SCHEDULED = 'scheduled'
RUNNING = 'running'
COMPLETED = 'completed'
FAILED = 'failed'
CANCELLED = 'cancelled'
STOPPED = 'stopped'
SKIPPED = 'skipped'

READY_STATES = frozenset([WAITING, SCHEDULED])
RUNNING_STATES = frozenset([WAITING, SCHEDULED, RUNNING])
FINISHED_STATES = frozenset([STOPPED, CANCELLED, FAILED, COMPLETED, SKIPPED])

def _format_args(args):
    argstring = ""

    for (arg, value, shorten) in args:
        val = str(value)

        if shorten == 1 and os.path.isabs(val):
            val = os.path.basename(val)

        argstring = ("%s %s %s" % (argstring, arg, val))

    return argstring


def log_job_info(log_file, job):
    '''Log the results of a job'''
    outputs = []
    msg = (
        "Job: {cmd}\n"
        "Submitted at: {started}\n"
        "Completed at: {ended}\n"
        "Execution time: {elapsed} sec\n"
        "Assigned to task: {taskid}\n"
        "Return status: {returned}\n"
        "Expected outputs: {outputs}\n"
        "Command Output:\n"
        "{output}")

    for item in job.outputs:
        if isinstance(item, list) and item[1]:
            outputs.append(item[0])
        else:
            outputs.append(item)

    job.info['outputs'] = ', '.join(outputs)
    description = '{0}\n'.format(job.description)
    body = msg.format(**job.info)

    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write(description)
        log_handle.write(body)
        log_handle.write('#' * 25 + '\n\n')

def log_skipped_job(log_file, job):
    '''Log a job that was skipped'''
    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write('{0}\n'.format(job.description))
        log_handle.write("Job: %s\n" % str(job))
        log_handle.write("Skipped: The analysis was previously generated.\n")
        log_handle.write('#' * 25 + '\n\n')

def log_not_run_job(log_file, job):
    '''Log a job that could not be run'''
    if not log_file or not os.path.isfile(log_file):
        return

    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write('{0}\n'.format(job.description))
        log_handle.write("Job: %s\n" % str(job))
        log_handle.write("The job was not run.\n")
        log_handle.write('#' * 25 + '\n\n')

class Job(object):
    def __init__(self, cmd, script, arguments, description=''):
        self.cmd = cmd
        self.script = script
        self.args = arguments
        self.inputs = []
        self.outputs = []
        self._status = 'scheduled'
        self._description = description
        self._info = {}
        self._errors = []
        self.attempts = 1
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
            logger.info("Checking return code status")
            if self.info:
                returned = self.info['returned']
                logger.info("Returned %s", returned)

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
        self.attempts = self.attempts + 1

    def failed(self):
        return self.attempts > self.options['retries']

    def __eq__(self, other):
        return (sorted(other.inputs) == sorted(self.inputs) and
                sorted(other.outputs) == sorted(self.outputs) and
                str(other) == str(self))

    def __repr__(self):
        return ' '.join([self.cmd, self.args])

    def __str__(self):
        return repr(self)

#FIXME: states for jobs should be decoupled from jobs
#TODO: Add proper dependency management to jobs
class Workflow(object):
    def __init__(self, name, jobs, log=None, priority=0):
        self.name = name
        self.log = log
        self.priority = priority
        self.jobs = tuple(jobs)
        self.available = jobs
        self.running = []
        self.completed = []
        self.status = core.Status.Initialized

    def update_status(self, job, info):
        '''Updates the status of the workflow'''
        #: Assign the info object to the job
        job.info = info

        #: Remove the job from the running list
        self.running.remove(job)

        #FIXME: add workflow change events
        #: Update the workflow log
        if self.log:
            log_job_info(self.log, job)

        #: Check that job returned successfully
        if info['returned'] != 0 or not job.completed():
            job.status = FAILED
            self.completed.append(job)
            self.status = core.Status.Failed
            return self.status

        #: Update the status to completed
        job.status = COMPLETED

        #: Check if the workflow finished
        if self._finished():
            self.status = core.Status.Completed
            return self.status

        #: Check that the workflow can proceed from this point
        if self._can_proceed():
            self.status = core.Status.Running
            return self.status
        else:
            self._failed()
            self.status = core.Status.Failed
            return self.status

    def next(self):
        '''Return the next set of available jobs'''
        available = []
        skipped = []

        for job in self.available:
            if job.completed():
                skipped.append(job)
                continue

            if job.ready() and job.status in READY_STATES:
                self.available.remove(job)
                self.running.append(job)
                available.append(job)
                job.status = RUNNING

        for job in skipped:
            self._skip(job)

        #: Check if any tasks are busy
        if available or self.running:
            self.status = core.Status.Running
        elif not self.available:
            #: Check if all jobs have been skipped
            self.status = core.Status.Completed
        else:
            self._failed()
            self.status = core.Status.Failed

        return available

    def cancel(self):
        ''' Sets the state of the workflow as cancelled'''
        self.status = core.Status.Cancelled

        for job in self.available:
            if job in RUNNING_STATES:
                job.status = CANCELLED

    def stop(self):
        ''' Sets the state of the workflow as stopped'''
        self.status = core.Status.Stopped

        for job in self.available:
            if job in RUNNING_STATES:
                job.status = STOPPED

    def state(self):
        """Returns the state of the workflow"""
        return [job.state for job in self.jobs]

    def _finished(self):
        """Returns True when all jobs have been finished"""
        return not self.available and not self.running

    def _can_proceed(self):
        """
        Returns whether the workflow can continue.
        """
        #: Get the number of running jobs
        total_running = len(self.running)

        #: Check if any jobs are still waiting
        waiting = [job for job in self.available if job.status in READY_STATES]

        #: Get the number of ready jobs
        total_ready = len([job for job in waiting if job.ready()]) > 0

        #: Proceed if a job is running or a job is ready
        return total_ready or total_running

    def _failed(self):
        '''Sets a job into the failed state'''
        for job in self.available:
            job.status = FAILED
            #FIXME: add workflow change events
            log_not_run_job(self.log, job)

    def _skip(self, job):
        '''Sets a job into a skipped state'''
        job.status = SKIPPED
        self.available.remove(job)
        self.completed.append(job)

    def status_message(self):
        prefix = "WORKFLOW{0}: " % self.name
        states = sorted(job.state for job in self.available + self.completed)

        #: summarize the number of jobs in each state
        fields = [",".join([state, len(jobs)]) for state,jobs in groupby(states)]

        #: summary of the workflows status
        summary = " ".join(fields)

        return prefix + summary

def filter_options(options):
    """
    Returns the set of filtered options that are specified
    """
    return {key : value for (key, value) in options.iteritems()
                if value is not None}

def validate_job(job_object):
    """
    Returns if the job is valid or gives a reason why invalid.
    """

    cmd = job_object.get('cmd', None)
    args = job_object.get('args', [])
    inputs = job_object.get('inputs', []) or []
    outputs = job_object.get('outputs', []) or []

    if not cmd:
        return (False, "The command name was not specified")

    if not isinstance(args, list):
        return (False, "The job expected a list of arguments")

    if not isinstance(inputs, list):
        return (False, "The job expected a list of inputs")

    if not isinstance(outputs, list):
        return (False, "The job expected a list of outputs")

    if any(item is None for item in inputs):
        return (False, "An input was invalid")

    if any(fp is None for fp in outputs):
        return (False, "An output was invalid")

    return (True, "The job has been validated")

def generate_workflow(workflow_object):
    '''Generates a workflow from a python object.'''
    logger.info("######### Generate Workflow  ##########")
    job_objects = workflow_object.get('jobs', [])

    if not job_objects:
        raise WorkflowError("The workflow does not contain any jobs.")

    name = workflow_object.get('name', 'unnamed')
    level = workflow_object.get('priority', 0)
    logfile = workflow_object.get('logfile', None)

    errors = []

    # Verify jobs and save errors
    for (index, job_object) in enumerate(job_objects):
        (valid, reason) = validate_job(job_object)

        if not valid:
            errors.append((index, reason))

    if errors:
        raise WorkflowError("%s jobs where not valid." % len(errors), errors)

    jobs = [generate_job(job_object) for job_object in job_objects]
    workflow = Workflow(name, jobs, log=logfile, priority=level)
    logger.info("WORKFLOW %s has been generated.", name)
    return workflow

def generate_job(job_object):
    """
    Returns a job generated from a python object
    """
    (cmd, script, args) = (job_object['cmd'], job_object['script'],
                           job_object.get('args', []))

    arg_string = _format_args(args)

    # Set the job_object description
    desc = job_object.get('description', '')
    new_job = Job(cmd, script, arg_string, description=desc)
    logger.debug("Creating job %s",  new_job.description)

    # Set the job_object options
    options = job_object.get('options', {})
    logger.info("Additional job options being set %s", options)
    new_job.options = filter_options(options)

    # Add inputs
    inputs = job_object.get('inputs', []) or []
    new_job.inputs.extend(sorted(inputs))

    # Add outputs
    outputs = job_object.get('outputs', []) or []
    new_job.outputs.extend(sorted(outputs))

    if 'overwrite' in job_object and int(job_object['overwrite']):
        logger.debug(("The job will overwrite previous"
            "results:\n%s"), new_job)
        new_job.clear()

    return new_job


class WorkflowError(Exception):
    def __init__(self, message, errors=None):
        Exception.__init__(self, message)
        self.errors = errors
