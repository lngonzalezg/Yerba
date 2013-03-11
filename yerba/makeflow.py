import sys, os, logging, tempfile

from services import WorkflowService

logger = logging.getLogger('yerba.makeflow')

class MakeflowBuilder():
    ''' Generates a workflow.'''
    def __init__(self, workflow):
        self.workflow = workflow
        self.jobs = []
        self.options = ["BATCH_OPTIONS="]

    def build_workflow(self):
        logger.info("Generating %s workflow", self.workflow)

        fp = None
        try:
            fp = open("%s.makeflow" % self.workflow, 'w')
            fp.write("RUN=/bin/bash\n")

            # Add options
            if len(self.options) > 1:
                fp.write(append_newline(" ".join(self.options)))

            # Add jobs
            workflow_job = "%s:%s\n\t$RUN %s\n"
            for job in self.jobs:
                outputs = " ".join(job.outputs)
                inputs = " ".join(job.inputs)
                fp.write(workflow_job % (outputs, inputs, job.script))
            fp.close()
        except Exception as e:
            logger.warn("Unable to generate workflow %s" % self.workflow)
            return

        logger.info("Generated %s.makeflow" % self.workflow)

    def add_option(self, value):
        self.options.append(value)

    def add_job(self, job):
        if job:
            self.jobs.append(job)

class Job():
    def __init__(self, outputs, inputs, script):
        self.script = script
        self.inputs = inputs
        self.outputs = outputs

class JobBuilder():
    def __init__(self, cmd, args, script=None, input_dir="", remote_dir="",
                 output_dir="", wildcard="*", prefix=""):

        self.cmd = os.path.abspath(cmd)
        self.script = script
        self.args = args

        self.remote_dir = remote_dir
        self.output_dir = output_dir
        self.input_dir = input_dir

        self.prefix = prefix
        self.wildcard = wildcard
        self.files = []
        self.prehooks = []
        self.posthooks = []
        self.temp_files = []
        self.temp_dir = tempfile.mkdtemp(prefix="backend-")
        self.cur_id = 0

    def build_job(self, work_file=None):
        script_name = "worker-" + str(self.cur_id) + ".sh"
        script = os.path.join(self.temp_dir, script_name)

        try:
            fp = open(script, 'w+')
            fp.write(append_newline("#!/bin/bash"))

            if work_file:
                name = os.path.basename(work_file).strip()
                wf = os.path.join(self.input_dir, name)
                if os.path.isfile(wf):
                    self.files.append((wf, False, False))
                    fp.write(append_newline("export WORK_FILE=%s" % name))
                else:
                    logger.warn(name + ": not a valid file.")

            # Generate prehooks
            for cmdstring in self.prehooks:
                prehook_msg = 'echo "running %s"\n' % cmdstring.strip()
                fp.write(prehook_msg)
                fp.write("bash %s" % cmdstring)

            # COMMAND
            logger.debug("Arguments: %s", self.args)
            argstring = ""

            for (arg, value, makeflow_format) in self.args:
                if int(makeflow_format) == 1:
                    value = value.replace("/", "_")

                argstring = ("%s %s %s" % (argstring, arg, value))

            cmdstring = "%s %s $WORK_FILE" % (self.cmd, argstring)
            cmd_status = append_newline('echo "running %s"' % self.cmd)
            fp.write(cmd_status)
            fp.write(append_newline(cmdstring))

            # Generate posthooks
            for cmdstring in self.posthooks:
                arg_index = cmdstring.find(" ")
                cmd = cmdstring[:arg_index]
                posthook_msg = 'echo "running %s"\n' % cmd.strip()
                fp.write(posthook_msg)
                fp.write("bash %s" % cmdstring)

            fp.close()
        except Exception as e:
            logger.exception("%s could not be written.", script_name)
            return None
        finally:
            self.cur_id = self.cur_id + 1
            self.temp_files.append(script_name)
            logger.info(script_name + " was added.")

        # Add files to the Task
        # TODO: fix cache and remote
        outputs = []
        inputs = [script]

        for (filename, is_output, cache) in self.files:
            if self.wildcard in filename:
            #    ext_index = name.rfind(".")
            #    filename = filename.replace(self.wildcard, name[:ext_index])
                filename = filename.replace(self.wildcard, self.prefix)

            if is_output:
                outputs.append(filename)
            else:
                inputs.append(filename)

        # FIXME: This check should be moved outside of this method.
        if len(inputs) < 2:
            logger.warn("Unable to build job: %s", self.cmd)
            return None

        logger.info("Built job %s", self.cmd)
        return Job(outputs, inputs, script)


    def add_hooks(self, hooks, source_dir="", before=True):
        if not hooks:
            return

        for cmdstring in hooks:
            cmdstring = cmdstring.strip()
            cmd = cmdstring.split()[0]

            if self.add_file(cmd, source_dir=source_dir):
                if before:
                    self.prehooks.append(append_newline(cmdstring))
                else:
                    self.posthooks.append(append_newline(cmdstring))
                logger.info(cmdstring + ": was successfully added for deployment.")
            else:
                logger.warn(cmd + ": will not be deployed.")

    def add_file_group(self, files, source_dir="", remote=False):
        if not files:
            return

        for f in files:
            self.add_file(f, source_dir=source_dir, remote=remote)

    def add_file(self, filename, source_dir="", remote=False):
        if not filename:
            logging.warn("The filename was NoneType.")
            return False

        filename = filename.strip()
        name = os.path.basename(filename)
        if source_dir:
            dfile = os.path.join(source_dir, name)
        else:
            dfile = name

        found = [os.path.isfile(filename), os.path.isfile(dfile)]
        success = True

        exist = [os.path.basename(fn) == name for (fn, t, c) in self.files]

        if remote and self.wildcard in filename:
            output_name = os.path.join(self.output_dir, filename)
            output_file = (output_name, True, False)
            self.files.append(output_file)
        elif remote:
            output_file = (filename, True, False)
            self.files.append(output_file)
        elif found[0]:
            input_file = (filename, False, True)
            self.files.append(input_file)
        elif found[1]:
            input_file = (dfile, False, True)
            self.files.append(input_file)
        else:
            logger.warn(name + ": does not exist.")
            success = False

        return success

    def cleanup(self):
        try:
            for tmpfile in self.temp_files:
                os.path.join(self.temp_dir, tmpfile)

            os.rmdir(self.temp_dir)
        except:
            print(("%s was not completely removed." % self.temp_dir))

class MakeflowService(WorkflowService):
    def get_status(self, workflow):
        logfile = "%s.makeflowlog" % workflow
        current_status = {"status" : "Scheduled" }
        lines = None

        if not os.path.exists(logfile):
            return current_status

        try:
            lines = open(logfile).readlines()
        except Exception as e:
            logger.exception(e)

        if lines:
            for line in lines:
                if line.startswith("#"):
                    current_status['status'] = line.split()[1]

        logger.info(current_status['status'])
        return current_status

    def create_workflow(self, workflow):
        wb = MakeflowBuilder("%s" % (workflow['name']))
        jobs = workflow['jobs']

        for job in jobs:
            cmdstring = job['cmdstring']
            cmd = cmdstring['cmd']
            script = cmdstring['script']
            args = cmdstring['args']
            
            if script:
                jb = JobBuilder(cmd, args, script=script)
            else:
                jb = JobBuilder(cmd, args)

            if 'inputs' in job:
                jb.add_file_group(job['inputs'])

            if 'outputs' in job:
                jb.add_file_group(job['outputs'], remote=True)

            wb.add_job(jb.build_job())

        wb.build_workflow()
        return "%s.makeflow" % (workflow['name'])

    # TODO: The makeflow options should be configurable.
    # @by Evan Briones
    # @on 3/7/2013
    def run_workflow(self, workflow):
        logger.info("Running workflow: %s", workflow)

        try:
            os.popen("makeflow -T wq -N coge -a -C localhost:1024 %s &" %
                    os.path.abspath(workflow))
        except:
            logger.exception("Unable to run workflow")

def append_newline(string):
     return string + "\n"
