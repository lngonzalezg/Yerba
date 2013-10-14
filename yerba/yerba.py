import argparse
import atexit
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
running = True

def listen_forever(port, options=None):
    WorkQueueService.set_project(options['queue_prefix'])
    wq = WorkQueueService()
    wq.workqueue_log(options['wqlog'])
    ServiceManager.register(wq)
    ServiceManager.start()

    connection_string = "tcp://*:{}".format(port)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.set(zmq.LINGER, 0)
    socket.bind(connection_string)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    atexit.register(shutdown)

    while True:
        ServiceManager.update()
        if socket in dict(poller.poll(timeout=10)):
            try:
                msg = socket.recv_json(flags=zmq.NOBLOCK)
            except Exception:
                msg = None
                logger.exception("The message was not parsed")

            response = None

            if not msg:
                logger.info("The message was not recieved.")
                continue

            with utils.ignored(RouteNotFound):
                response = dispatch(msg)

            if not response:
                logger.info("Invalid request: %s", msg)
                response = {"status" : "error"}

            try:
                socket.send_json(response, flags=zmq.NOBLOCK)
            except zmq.Again:
                logger.exception("Failed to respond with response %s",
                    response)
def shutdown():
    ServiceManager.stop()

@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    status = WorkflowManager.submit(data)
    return {"status" : core.status_name(status)}

@route("cancel")
def cancel_workflow(data):
    '''Cancels the job if it is running.'''
    try:
        identity = data['id']
        status = WorkflowManager.cancel(identity)
        logger.info(core.status_message(identity, status))
        return {"status" : core.status_name(status)}
    except KeyError:
        return {"status" : 'NotFound'}

@route("get_status")
def get_workflow_status(data):
    '''Gets the status of the workflow.'''
    try:
        identity = data['id']
        (status, jobs) = WorkflowManager.status(identity)
        logger.info(core.status_message(identity, status))
        return {"status" : core.status_name(status), "jobs" : jobs}
    except KeyError:
        return {"status" : 'NotFound', "jobs" : {}}
