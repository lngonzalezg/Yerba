import argparse
import atexit
import json
import logging
import os
import time
from time import sleep

import zmq
from managers import (ServiceManager, WorkflowManager)
from routes import (route, dispatch, RouteNotFound)
from workqueue import WorkQueueService

import utils
import core

logger = logging.getLogger('yerba')
running = True
decoder = json.JSONDecoder()

def listen_forever(config):
    wq = WorkQueueService(dict(config.items('workqueue')))
    ServiceManager.register(wq)
    ServiceManager.start()

    connection_string = "tcp://*:{}".format(config.get('yerba', 'port'))
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.set(zmq.LINGER, 0)
    socket.bind(connection_string)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    atexit.register(shutdown)

    while True:
        try:
            if socket in dict(poller.poll(timeout=10)):
                msg = None
                response = None

                try:
                    data = socket.recv_string()
                    logger.debug("ZMQ: Recieved %s", data)
                    msg = decoder.decode(data)
                except Exception:
                    logger.exception("ZMQ: The message was not parsed")

                if not msg:
                    logger.info("The message was not recieved.")
                else:
                    try:
                        response = dispatch(msg)
                    except:
                        logger.exception("EXCEPTION")

                logger.info("#### END REQUEST ####")

                if not response:
                    logger.info("Invalid request: %s", msg)
                    response = {"status" : "error"}

                try:
                    logger.info("Sending Response")
                    socket.send_json(response, flags=zmq.NOBLOCK)
                except zmq.Again:
                    logger.exception("Failed to respond with response %s",
                        response)
                finally:
                    logger.info("Finished processing the response")
            else:
                try:
                    ServiceManager.update()
                except:
                    logger.exception("WORKQUEUE: Update error occured")
        except:
            logger.exception("EXPERIENCED AN ERROR!")

        sleep(50.0/1000.0)

def shutdown():
    ServiceManager.stop()

@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    logger.info("##### WORKFLOW SCHEDULING #####")
    status = WorkflowManager.submit(data)
    return {"status" : core.status_name(status)}

@route("cancel")
def cancel_workflow(data):
    '''Cancels the job if it is running.'''
    logger.info("##### WORKFLOW CANCELLATION #####")
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
    logger.info("##### WORKFLOW STATUS CHECK #####")
    try:
        identity = data['id']
        (status, jobs) = WorkflowManager.status(identity)
        logger.info(core.status_message(identity, status))
        return {"status" : core.status_name(status), "jobs" : jobs}
    except KeyError:
        return {"status" : 'NotFound', "jobs" : {}}
