import argparse
from json import (JSONEncoder, JSONDecoder)
import json as js
import logging
import logging.handlers as loghandlers
import os
import time
from string import Template

from zmq import (Context, REP, NOBLOCK, ZMQError)

from services import Status
from managers import (ServiceManager, WorkflowManager, Router, route,
                      RequestError, RouteNotFound)
from workqueue import WorkQueueService
import utils

DEFAULT_ZMQ_PORT = 5151

# Setup Logging
if os.path.exists("logging.conf"):
   logging.config.fileConfig("logging.conf")
   logger = logging.getLogger('yerba')
else:
    logger = logging.getLogger('yerba')
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')

    filehandler = loghandlers.TimedRotatingFileHandler('yerba.log', 'midnight')
    filehandler.setLevel(logging.DEBUG)
    filehandler.setFormatter(fmt)

    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.INFO)
    streamhandler.setFormatter(fmt)

    logger.addHandler(filehandler)
    logger.addHandler(streamhandler)

@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    return WorkflowManager.submit(data)

@route("cancel")
def terminate_workflow(id):
    '''Terminates the job if it is running.'''
    status = WorkflowManager.cancel(id)
    logger.info(_status_messages(id))

    return status

_status_messages = {
    Status.Attached: "The workflow %s is Attached",
    Status.Scheduled: "The workflow %s has been scheduled.",
    Status.Completed: "The workflow %s was completed.",
    Status.Terminated: "The workflow %s has been terminated.",
    Status.Failed: "The workflow %s failed.",
    Status.Error: "The workflow %s has errors.",
    Status.NotFound: "The workflow %s was not found.",
    Status.Running: "The workflow %s is running."
}

@route("get_status")
def get_workflow_status(id):
    '''Gets the status of the workflow.'''
    status = WorkflowManager.status(id)
    logger.info(_status_messages[status], id)

    return status


def listen_forever(connection_string):
    context = Context()
    socket = context.socket(REP)
    socket.bind(connection_string)

    while True:
        ServiceManager.update()

        with utils.ignored(ZMQError):
            msg = socket.recv(flags=NOBLOCK)

            try:
                request_object = JSONDecoder().decode(msg.decode("ascii"))
            except:
                request_object = None
                logger.exception("Request was not able to be decoded.")

            try:
                status = Router.dispatch(request_object)
            except (RequestError, RouteNotFound):
                logger.exception("The request failed.")
                status = Status.Error

            response = JSONEncoder().encode({"status" : status})
            socket.send_unicode(response)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Processes bioinformatic jobs.')

    parser.add_argument('--port', default=DEFAULT_ZMQ_PORT)
    parser.add_argument('--log')
    parser.add_argument('--makeflow', action='store_true')
    args = parser.parse_args()

    if args.log:
        log_level = getattr(logging, args.log.upper(), None)

        if not isinstance(log_level, int):
            raise ValueError('Invalid log level: %s' % log_level)
        logger.setLevel(log_level)

    ServiceManager.register(WorkQueueService())
    ServiceManager.start()
    listen_forever(Template("tcp://*:$port").substitute(port=args.port))
