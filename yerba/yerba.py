import argparse
import logging
import os
import time

import zmq
from managers import (ServiceManager, WorkflowManager)
from routes import (route, dispatch, RouteNotFound)
from workqueue import WorkQueueService

import utils
import core

logger = logging.getLogger('yerba')

def listen_forever(port, options=None):
    WorkQueueService.set_project(options['queue_prefix'])
    ServiceManager.register(WorkQueueService())
    ServiceManager.start()

    connection_string = "tcp://*:{}".format(port)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(connection_string)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while True:
        ServiceManager.update()

        if socket in dict(poller.poll(timeout=10)):
            msg = socket.recv_json(flags=zmq.NOBLOCK)
            status = None

            with utils.ignored(RouteNotFound):
                status = dispatch(msg)

            if not status:
                status = core.Status.Error

            socket.send_json({"status" : status})


@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    return WorkflowManager.submit(data)

@route("cancel")
def terminate_workflow(id):
    '''Terminates the job if it is running.'''
    status = WorkflowManager.cancel(id)
    logger.info(status_messages(id, status))

    return status

@route("get_status")
def get_workflow_status(id):
    '''Gets the status of the workflow.'''
    status = WorkflowManager.status(id)
    logger.info(status_messages(id, status))

    return status
