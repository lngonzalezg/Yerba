import argparse
from json import (JSONEncoder, JSONDecoder)
import json as js
import logging
import logging.handlers as loghandlers
import os
import time
from string import Template

from zmq import (Context, REP)

from services import (WorkQueueService, Status)
from managers import (ServiceManager, WorkflowManager, Router, route,
                      RequestError, DispatchRouteNotFoundException)

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
    status = WorkflowManager.submit(data)

    if status == Status.Attached:
        logger.info("Attached workflow")
    else:
        logger.info("Scheduled workflow")

    return status

@route("cancel")
def terminate_workflow(id):
    '''Terminates the job if it is running.'''
    status = WorkflowManager.cancel(id)

    if status == Status.NotFound:
        logger.info("Unable to cancel workflow not found.")
    else:
        logger.info("Workflow was cancelled")

    return status

@route("get_status")
def get_workflow_status(id):
    '''Gets the status of the workflow.'''
    status = WorkflowManager.status(id)

    if status == Status.Attached:
        logger.info("The workflow %s is Attached", id)
    elif status == Status.Scheduled:
        logger.info("The workflow %s is Scheduled", id)
    elif status == Status.Completed:
        logger.info("The workflow %s is Completed", id)
    elif status == Status.Waiting:
        logger.info("The workflow %s is Waiting", id)
    elif status == Status.Terminated:
        logger.info("The workflow %s was Terminated", id)
    elif status == Status.Running:
        logger.info("The workflow %s is Running", id)
    elif status == Status.Error:
        logger.info("The workflow %s has errors", id)
    elif status == Status.NotFound:
        logger.info("The workflow %s was not found", id)
    else:
        logger.info("The workflow %s is in an unknown status %d", id, status)

    return status

def listen_forever(connection_string):
    encoder = JSONEncoder()
    decoder = JSONDecoder()
    context = Context()

    socket = context.socket(REP)
    socket.bind(connection_string)

    while True:
        request = socket.recv()

        try:
            request_object = decoder.decode(request.decode("ascii"))
        except:
            request_object = None
            logger.exception("Request was not able to be decoded.")

        try:
            status = Router.dispatch(request_object)
        except RequestError as e:
            logger.exception("The request was invalid")
            status = Status.Error
        except DispatchRouteNotFoundException as e:
            logger.exception("The request is not supported.")
            status = Status.Error

        response = encoder.encode({"status" : status})
        socket.send_unicode(response)
        ServiceManager.update()

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

    connection_string = Template("tcp://*:$port").substitute(port=args.port)

    ServiceManager.register(WorkQueueService())
    ServiceManager.start()
    listen_forever(connection_string)
