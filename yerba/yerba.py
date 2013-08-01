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
            response = None

            with utils.ignored(RouteNotFound):
                response = dispatch(msg)

            if not response:
                response = {"status" : 'error'}

            socket.send_json(response)


@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    status = WorkflowManager.submit(data)
    return {"status" : core.status_name(status)}

@route("cancel")
def terminate_workflow(id):
    '''Terminates the job if it is running.'''
    status = WorkflowManager.cancel(id)
    logger.info(core.status_message(id, status))

    return {"status" : core.status_name(status)}

@route("get_status")
def get_workflow_status(id):
    '''Gets the status of the workflow.'''
    (status, data) = WorkflowManager.status(id)
    logger.info(core.status_message(id, status))

    return {"status" : core.status_name(status), "jobs" : data}
