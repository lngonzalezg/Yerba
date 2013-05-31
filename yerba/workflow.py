import heapq
import logging
import os

from service import Service
logger = logging.getLogger('yerba.workflow')

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
    def __init__(self, cmd, script, arguments):
        self.cmd = cmd
        self.script = script
        self.args = arguments
        self.inputs = []
        self.outputs = []

    def add_inputs(self, files):
        self._add_files(files)

    def add_outputs(self, files):
        self._add_files(files, output_file=True)

    def _add_files(self, files, output_file=False):
        if isinstance(files, list) and output_file:
            self.outputs.extend(files)
        elif isinstance(files, list):
            self.inputs.extend(files)

    def completed(self):
        '''Returns whether or not the job was completed.'''
        missing = [fp for fp in self.outputs if not os.path.isfile(fp)]
        return not bool(missing)

    def ready(self):
        '''Returns that the job has its input files and is ready.'''
        missing = [fp for fp in self.inputs if not os.path.isfile(fp)]
        return not bool(missing)

    def __repr__(self):
        return ' '.join([self.cmd, _format_args(self.args)])

    def __str__(self):
        return repr(self)

def _parse_cmdstring(cmdstring):
    cmd = None
    script = None
    args = None

    if 'cmd' in cmdstring:
        cmd = cmdstring['cmd']

    if 'script' in cmdstring:
        script = cmdstring['script']

    if 'args' in cmdstring:
        args = cmdstring['args']

    return (cmd, script, args)

def generate_workflow(pyobject):
    '''Generates a workflow from a python object.'''
    if 'name' not in pyobject or 'jobs' not in pyobject:
        raise InvalidWorkflowException("The workflow format was invalid.")

    if not len(pyobject['jobs']):
        raise EmptyWorkflowException("The workflow does not contain any jobs.")

    name = pyobject['name']
    priority = 0
    jobs = []
    logname = "%s.log" % name
    logfile = os.path.join(pyobject['filepath'], logname)

    for job in pyobject['jobs']:
        if ('cmdstring' not in job or 'inputs' not in job
            or 'outputs' not in job):
            raise InvalidJobException("The job format was invalid.")

        (cmd, script, args) = _parse_cmdstring(job['cmdstring'])
        new_job = Job(cmd, script, args)

        if 'inputs' in job:
            new_job.add_inputs(job['inputs'])

        if 'outputs' in job:
            new_job.add_outputs(job['outputs'])

        if 'overwrite' in job and int(job['overwrite']):
            logging.info("The job will be restarted.")
            new_job.restart = True

        jobs.append(new_job)

    return (name, logfile, priority, jobs)

class Workflow(object):
    def on_schedule(self):
        '''Called to schedule the workflow.'''

    def on_submit(self):
        '''Called when the workflow is scheduled.'''

    def on_complete(self):
        '''Cancels the workflow.'''

    def on_cancel(self):
        '''Cancels the workflow.'''

    def on_status(self):
        '''Request the status of the workflow'''

class WorkflowService(Service):
    def initialize(self):
        self.queue = {}

    def submit(self, workflow):
        '''Submits a workflow to the queue.'''
        if workflow.name in self.queue:
            return service.COMPLETED

    def update(self):
        '''Updates the scheduler.'''


class InvalidJobException(ValueError):
    pass

class InvalidWorkflowException(ValueError):
    pass

class EmptyWorkflowException(ValueError):
    pass
