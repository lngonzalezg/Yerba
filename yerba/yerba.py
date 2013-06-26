import argparse
import logging
import os
import time

from zmq import (Context, REP, NOBLOCK, ZMQError)
from services import Status
from managers import (ServiceManager, WorkflowManager, Router, route,
                      RequestError, RouteNotFound)
from workqueue import WorkQueueService
import utils

logger = logging.getLogger('yerba')

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

def listen_forever(port, options=None):
    WorkQueueService.set_project(options['queue_prefix'])
    ServiceManager.register(WorkQueueService())
    ServiceManager.start()

    connection_string = "tcp://*:{}".format(port)
    context = Context()
    socket = context.socket(REP)
    socket.bind(connection_string)

    while True:
        ServiceManager.update()
        status = None

        with utils.ignored(ZMQError):
            msg = socket.recv_json(flags=NOBLOCK)

            with utils.ignored(RequestError, RouteNotFound):
                status = Router.dispatch(msg)

            if not status:
                status = Status.Error

            socket.send_json({"status" : status})

        if not status:
            time.sleep(1)

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

@route("get_status")
def get_workflow_status(id):
    '''Gets the status of the workflow.'''
    status = WorkflowManager.status(id)
    logger.info(_status_messages[status], id)

    return status
