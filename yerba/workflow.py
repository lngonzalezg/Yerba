import heapq
import logging

from service import Service
logger = logging.getLogger('yerba.workflow')

class Workflow(object):
    def __init__(self, name, priority=0):
        self.name = name
        self.options = []
        self.jobs = []
        self.priority  = priority

    def add_job(self, jobs):
        if isinstance(jobs, list):
            self.jobs.extend(jobs)
        else:
            self.jobs.append(jobs)

    def __str__(self):
        return self.name

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

class WorkflowService(Service):
    group = "workflow"

    def get_status(self, workflow):
        ''' Get the current status of the workflow.'''
        pass

    def create_workflow(self, workflow):
        ''' Given a JSON workflow format it will construct the workflow.'''
        pass

    def run_workflow(self, workflow):
        ''' Executes a given workflow.'''
        pass
