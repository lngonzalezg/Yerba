import logging
import os

from workflow import Workflow, WorkflowService, Job
from managers import ServiceManager

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
        if makeflow_format == 1:
            val = os.path.basename(str(value))
        else:
            val = str(value)

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

        if 'logfile' in blob and blob['logfile']:
            workflow.logfile = blob['logfile']
        else:
            logger.info("Using default log file.")
            logname = "%s.log" % workflow.name
            workflow.logfile = os.path.join(workflow.filepath, logname)

        workflow.restart = False

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

            if 'overwrite' in job and int(job['overwrite']):
                workflow.restart = True
                for output in job.outputs:
                    if os.path.isfile(output):
                        os.remove(output)
            workflow.add_job(new_job)

        return workflow


    def generate(self):
        """ Generates a makeflow file. """
        logfile = "%s.makeflowlog" % str(self)
        overwrite = False

        if os.path.isfile(str(self)) and not overwrite:
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

NODE_FIELD = "NODE"
PARENTS_FIELD = "PARENTS"
SOURCES_FIELD = "SOURCES"
TARGETS_FIELD = "TARGETS"
COMMAND_FIELD = "COMMAND"

# Status fields
WAITING_FIELD = "WAITING"
STARTED_FIELD = "STARTED"
COMPLETED_FIELD = "COMPLETED"
ABORTED_FIELD = "ABORTED"
FAILED_FIELD = "FAILED"

class MakeflowLog():
    def __init__(self, logfile):
        self.logfile = logfile
        self.runs = 0

    def parse(self):
        try:
            lines = open(self.logfile).readlines()
        except:
            logger.exception("Unable to read makeflow log %s.", self.logfile)
            return (None, None)

        status_service = ServiceManager.get("status", "internal")

        # Use two stacks to gather nodes
        item_stack = []
        node_stack = []

        # status messages
        status_messages = []

        for line in lines:
            if not line.startswith("#"):
                status_messages.append(line.split())
            else:
                # Strip off comment character and split
                items = line[1:].split()
                field_type = items[0]
                timestamp = int(items[1])

                if field_type == STARTED_FIELD:
                    self.started = timestamp
                    self.status = status_service.STARTED
                elif field_type == WAITING_FIELD:
                    self.ended = timestamp
                    self.status = status_service.WAITING
                elif field_type == COMPLETED_FIELD:
                    self.ended = timestamp
                    self.status = status_service.COMPLETED
                    self.runs = self.runs + 1
                elif field_type == ABORTED_FIELD:
                    self.ended = timestamp
                    self.status = status_service.CANCELLED
                    self.runs = self.runs + 1
                elif field_type == FAILED_FIELD:
                    self.ended = timestamp
                    self.status = status_service.FAILED
                    self.runs = self.runs + 1
                elif field_type == NODE_FIELD:
                    if len(item_stack) > 1:
                        node_stack.append(item_stack)
                        item_stack = []
                    item_stack.append((field_type, items[1], items[2:]))
                else:
                    item_stack.append((field_type, items[1], items[2:]))

        # Add the last node to the stack
        if len(item_stack) > 1:
            node_stack.append(item_stack)

        return (status_messages, self._create_graph(node_stack))

    def current_status(self, with_jobs=False):
        (status_messages, graph_info) = self.parse()

        if status_messages:
            (timestamp, node_id, new_state, job_id,
            nodes_waiting, nodes_running, nodes_complete,
            nodes_failed, nodes_aborted,
            node_id_counter) = (int(item) for item in status_messages[-1])

            if (hasattr(self, 'ended') and self.ended >= timestamp
                and self.runs == 0):
                status = self.status
            elif with_jobs:
                status_service = ServiceManager.get("status", "internal")
                status = status_service.RUNNING
            else:
                status = new_state
        else:
            status_service = ServiceManager.get("status", "internal")
            status = status_service.WAITING

        return status

    def _create_graph(self, nodes):
        vertices = []
        edges = []

        for (node, parents, sources, targets, cmd) in reversed(nodes):
            vertices.append(' '.join(node[2]))
            edges.append(parents[2])

        return (vertices, edges)

class MakeflowService(WorkflowService):
    name = "makeflow"

    def get_status(self, workflow):
        logfile = "%s.makeflowlog" % workflow
        status_service = ServiceManager.get("status", "internal")

        lines = None
        if not os.path.exists(logfile):
            return status_service.SCHEDULED

        makeflow_log = MakeflowLog(logfile)

        status_code = makeflow_log.current_status()
        retries = makeflow_log.runs
        message = status_service.status_message(status_code)

        if status_code == status_service.STARTED:
            pass
           # fp.write("Workflow status was [%s]\n\n" % status)
        if status_code == status_service.WAITING:
            pass
           # fp.write("Finished workflow %s\n\n" % workflow.name)
        elif status_code == status_service.RUNNING:
            pass
        elif status_code == status_service.COMPLETED:
            pass
        elif status_code == status_service.FAILED:
            pass
        elif status_code == status_service.ABORTED:
            pass
           # fp.write("Workflow %s Aborted\n\n" % workflow.name)
        else:
            pass

        logger.info(message)
        return status_code

    def create_workflow(self, data):
        workflow = Makeflow.from_blob(data)

        try:
            workflow.generate()
        except:
            logger.exception("%s could not be generated", workflow.name)
            os.remove(str(workflow))

            workflow = None

        return workflow

    # TODO: The makeflow options should be configurable.
    # @by Evan Briones
    # @on 3/7/2013
    def run_workflow(self, workflow):
        logger.info("Running workflow: %s", workflow)

        status_service = ServiceManager.get("status", "internal")
        status_code = status_service.SCHEDULED
        logfile = "%s.makeflowlog" % workflow

        if os.path.exists(logfile):
            makeflow_log = MakeflowLog(logfile)
            status_code = makeflow_log.current_status()

        if ((not status_code == status_service.COMPLETED and
            not status_code == status_service.RUNNING) or
            workflow.restart):
            try:
                with open(workflow.logfile, 'a') as fp:
                    fp.write("Started workflow %s\n" % workflow.name)

                os.popen("makeflow -T wq -N coge -a -C localhost:1024 -z %s &" %
                        workflow)
            except:
                logger.exception("Unable to run workflow")
