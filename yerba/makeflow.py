import logging
import os

from workflow import Workflow, WorkflowService, Job

logger = logging.getLogger('yerba.makeflow')

class GeneratedFile(object):
    __slots__ = ['fp']

    def __init__(self, filename, options):
        try:
            self.fp = open(filename, options)
        except:
            logging.exception('Unable to open file %s', filename);

    def __enter__(self):
        if hasattr(self, 'fp'):
            return self.fp
        else:
            return None

    def __exit__(self, *exc_info):
        self.fp.close()

SEPERATOR = '->'
FILE_LINE_FORMAT = "%s:%s\n"
CMDLINE_FORMAT = "\t%s %s\n"

def format_job(job):
    job_string = (_format_fileline(job.inputs, job.outputs),
                _format_cmdline(job.cmd, job.args))
    return ''.join(job_string)

def _format_fileline(inputs, outputs):
    files = (_format_files(outputs), _format_files(inputs))
    return FILE_LINE_FORMAT % files

def _format_files(files):
    return ' '.join(_format_file_mapping(f) for f in files)

def _format_file_mapping(f):
    return SEPERATOR.join((f, os.path.basename(f)))

def _format_cmdline(cmd, args):
    return CMDLINE_FORMAT % (cmd, _format_args(args))

def _format_args(args):
    argstring = ""

    for (arg, value, makeflow_format) in args:
        val = os.path.basename(str(value))
        argstring = ("%s %s %s" % (argstring, arg, val))

    return argstring

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


class Makeflow(Workflow):
    ''' Generates a workflow.'''

    @classmethod
    def from_blob(cls, blob):
        if 'name' not in blob:
            logger.warn("Unspecified workflow name.")
            return None

        workflow = Makeflow(blob['name'])

        if 'jobs' not in blob:
            logger.warn("The workflow does not contain any jobs.")
            return workflow

        if 'filepath' not in blob:
            logger.warn("Filepath for makeflow was not specified.")
        else:
            workflow.filepath = blob['filepath']

        for job in blob['jobs']:
            if 'cmdstring' not in job:
                logger.warn("Invalid job.")
                continue

            (cmd, script, args) = _parse_cmdstring(job['cmdstring'])
            new_job = Job(cmd, script, args)

            if 'inputs' in job:
                new_job.add_inputs(job['inputs'])

            if 'outputs' in job:
                new_job.add_outputs(job['outputs'])

            workflow.add_job(new_job)

        return workflow


    def generate(self):
        """ Generates a makeflow file. """
        if os.path.isfile(str(self)):
            logger.info("%s has already been generated.", self.name)
            return

        with GeneratedFile(str(self), 'w+') as fp:
            # Add options
            if self.options:
                for opt in self.options:
                   fp.write('%s\n' % opt)

            for job in self.jobs:
                fp.write(format_job(job))

            logger.info("Generated %s" % self)

    def cleanup(self):
        """ Removes a makeflow log files. """
        pass

    def __str__(self):
        makeflow = "%s.makeflow" % self.name

        if hasattr(self, 'filepath'):
            filename = os.path.join(self.filepath, makeflow)
        else:
            filename = os.path.abspath(makeflow)

        return filename

class MakeflowService(WorkflowService):
    name = "makeflow"

    def get_status(self, workflow):
        logfile = "%s.makeflowlog" % workflow
        current_status = {"status" : "Scheduled" }
        lines = None

        if not os.path.exists(logfile):
            return current_status

        try:
            lines = open(logfile).readlines()
        except:
            logger.exception("Unable to read makeflow file %s.", logfile)

        if lines:
            for line in lines:
                if line.startswith("#"):
                    current_status['status'] = line.split()[1]

        logger.info(current_status['status'])
        return current_status

    def create_workflow(self, data):
        workflow = Makeflow.from_blob(data)
        workflow.generate()
        return workflow

    # TODO: The makeflow options should be configurable.
    # @by Evan Briones
    # @on 3/7/2013
    def run_workflow(self, workflow):
        logger.info("Running workflow: %s", workflow)
        try:
            os.popen("makeflow -T wq -N coge -a -C localhost:1024 %s &" %
                    workflow)
        except:
            logger.exception("Unable to run workflow")
