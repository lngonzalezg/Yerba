import heapq
import logging
import os

from services import Service
import utils
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
    def __init__(self, cmd, script, arguments, retries=3):
        self.cmd = cmd
        self.script = script
        self.args = arguments
        self.inputs = []
        self.outputs = []
        self.retries = retries

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
        raise WorkflowError("The workflow format was invalid.")

    if not len(pyobject['jobs']):
        raise WorkflowError("The workflow does not contain any jobs.")

    name = pyobject['name']
    priority = 0
    jobs = []
    logfile = pyobject['logfile']

    for job in pyobject['jobs']:
        if ('cmdstring' not in job or 'inputs' not in job
            or 'outputs' not in job):
            raise JobError("The job format was invalid.")

        (cmd, script, args) = _parse_cmdstring(job['cmdstring'])
        new_job = Job(cmd, script, args)

        if not cmd:
            raise JobError("The command name is NoneType.")
        if not args:
            raise JobError("The arguments are NoneType.")

        if 'inputs' in job and job['inputs']:
            if any(fp is None for fp in job['inputs']):
                raise JobError("Workflow %s has a NoneType input" % name)

            inputs = [str(item) for item in job['inputs']]
            new_job.inputs.extend(sorted(inputs))


        if 'outputs' in job:
            if any(fp is None for fp in job['outputs']):
                raise JobError("Workflow %s has a NoneType output" % name)

            outputs = [str(item) for item in job['outputs']]
            new_job.outputs.extend(sorted(outputs))

        if 'overwrite' in job and int(job['overwrite']):
            logger.info("The job will be restarted.")
            new_job.clear()

        jobs.append(new_job)

    return (name, logfile, priority, jobs)

class JobError(ValueError):
    pass

class WorkflowError(ValueError):
    pass
