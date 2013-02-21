import sys, os, logging, tempfile

class MakeflowBuilder():
    ''' Generates a workflow.'''
    def __init__(self, workflow):
        self.workflow = workflow
        self.jobs = []
        self.options = ["BATCH_OPTIONS="]

    def build_workflow(self):
        logging.info("Generating %s workflow", self.workflow)

        try:
            fp = open("%s.makeflow" % self.workflow, 'w')
            fp.write("RUN=/bin/bash\n")

            # Add options
            if len(self.options) > 1:
                fp.write(" ".join(self.options) + "\n")

            # Add jobs
            workflow_job = "%s:%s\n\t$RUN %s\n"
            for job in self.jobs:
                outputs = " ".join(job.outputs)
                inputs = " ".join(job.inputs)
                fp.write(workflow_job % (outputs, inputs, job.script))

        except Exception as e:
            logging.warn("Unable to generate workflow %s" % self.workflow)
            return
        finally:
            fp.close()

        logging.info("Generated %s.makeflow" % self.workflow)

    def add_option(self, value):
        self.options.append(value)

    def add_job(self, job):
        self.jobs.append(job)

class Job():
    def __init__(self, outputs, inputs, script):
        self.script = script
        self.inputs = inputs
        self.outputs = outputs
        self.inputs.append(script)

class JobBuilder():
    def __init__(self, cmdline, input_dir="", remote_dir="", output_dir="",
                 wildcard="*", prefix=""):

        self.remote_dir = remote_dir
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.prefix = prefix
        self.files = []
        self.prehooks = []
        self.posthooks = []
        self.temp_files = []
        self.temp_dir = tempfile.mkdtemp(prefix="backend-")
        self.wildcard = wildcard
        self.cur_id = 0

        cmd, sep, self.args = cmdline.partition(" ")
        cmdpath = os.path.abspath(cmd)
        self.cmd = os.path.basename(cmd)

        #if os.path.isfile(cmdpath):
        #    self.add_file(cmdpath)
        #else:
        #    logging.warn(self.cmd + ": will not be added as a file.")

    def build_job(self, work_file=None):
        script_name = "worker-" + str(self.cur_id) + ".sh"
        script = os.path.join(self.temp_dir, script_name)

        try:
            fp = open(script, 'w+')
            fp.write(self.append_newline("#!/bin/bash"))

            if work_file:
                name = os.path.basename(work_file).strip()
                wf = os.path.join(self.input_dir, name)
                if os.path.isfile(wf):
                    self.files.append((wf, False, False))
                    fp.write(self.append_newline("export WORK_FILE=%s" % name))
                else:
                    logging.warn(name + ": not a valid file.")

            # Generate prehooks
            for cmdstring in self.prehooks:
                prehook_msg = 'echo "running %s"\n' % cmdstring.strip()
                fp.write(prehook_msg)
                fp.write("bash %s" % cmdstring)

            # COMMAND
            cmdstring = "%s %s $WORK_FILE" % (self.cmdline, self.args)
            cmd_status = 'echo "running %s"\n' % self.cmd
            fp.write(cmd_status)
            fp.write(self.append_newline(cmdstring))

            # Generate posthooks
            for cmdstring in self.posthooks:
                arg_index = cmdstring.find(" ")
                cmd = cmdstring[:arg_index]
                posthook_msg = 'echo "running %s"\n' % cmd.strip()
                fp.write(posthook_msg)
                fp.write("bash %s" % cmdstring)
        except Exception as e:
            print(("Exception: %s" % str(e)))
            logging.warn(script_name + ": task could not be written.")
            return None
        finally:
            self.cur_id = self.cur_id + 1
            self.temp_files.append(script_name)
            logging.info(script_name + " was added.")
            fp.close()

        # Add files to the Task
        # TODO: fix cache and remote
        outputs = []
        inputs = []

        for (localfile, is_output, cache) in self.files:
            if self.wildcard in localfile:
            #    ext_index = name.rfind(".")
            #    filename = filename.replace(self.wildcard, name[:ext_index])
                filename = filename.replace(self.wildcard, self.prefix)

            filename = os.path.basename(localfile)
            remote = os.path.join(self.remote_dir, filename)

            if cache:
                outputs.append(filename)
            else:
                inputs.append(filename)

            # TODO: do we still need localfile name
            #worker.specify_file(localfile, filename,
            #    type=queue_type, cache=cache)

        return Job(outputs, inputs, script)

    def append_newline(self, string):
        return string + "\n"

    def add_hooks(self, hooks, source_dir="", before=True):
        if not hooks:
            return

        for cmdstring in hooks:
            cmdstring = cmdstring.strip()
            cmd = cmdstring.split()[0]

            if self.add_file(cmd, source_dir=source_dir):
                if before:
                    self.prehooks.append(self.append_newline(cmdstring))
                else:
                    self.posthooks.append(self.append_newline(cmdstring))
                logging.info(cmdstring + ": was successfully added for deployment.")
            else:
                logging.warn(cmd + ": will not be deployed.")

    def add_file_group(self, files, source_dir="", remote=False):
        if not files:
            return

        for f in files:
            self.add_file(f, source_dir=source_dir, remote=remote)

    def add_file(self, filename, source_dir="", remote=False):
        print(filename)
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
        elif any(exist) or (remote and found):
            logging.warn(name + ": this result file already exists.")
            success = False
        elif not found and remote:
            output_name = os.path.join(self.output_dir, filename)
            output_file = (output_name, True, False)
            self.files.append(output_file)
        elif found[0]:
            input_file = (filename, False, True)
            self.files.append(input_file)
        elif found[1]:
            input_file = (dfile, False, True)
            self.files.append(input_file)
        else:
            logging.warn(name + ": does not exist.")
            success = False

        return success

    def cleanup(self):
        try:
            for tmpfile in self.temp_files:
                os.path.join(self.temp_dir, tmpfile)

            os.rmdir(self.temp_dir)
        except:
            print(("%s was not completely removed." % self.temp_dir))
