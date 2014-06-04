import argparse
import atexit
import json
import logging
from time import sleep

import zmq
from yerba.core import status_message, status_name
from yerba.managers import (ServiceManager, WorkflowManager)
from yerba.routes import (route, dispatch)
from yerba.workflow import WorkflowError
from yerba.workqueue import WorkQueueService

logger = logging.getLogger('yerba')
running = True
decoder = json.JSONDecoder()

def listen_forever(config):
    wq = WorkQueueService(dict(config.items('workqueue')))
    ServiceManager.register(wq)
    ServiceManager.start()
    WorkflowManager.connect(config.get('db', 'path'))

    connection_string = "tcp://*:{}".format(config.get('yerba', 'port'))
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.set(zmq.LINGER, 0)
    socket.bind(connection_string)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    atexit.register(shutdown)

    while running:
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

        # Sleep for 5 milliseconds
        sleep(0.05)


@route("shutdown")
def shutdown():
    '''Shutdowns down the daemon'''
    running = False
    ServiceManager.stop()

#XXX: Add reporting information
@route("health")
def get_health(data):
    logger.info("#### HEALTH CHECK #####")
    return  {"status" : "OK" }

@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    logger.info("##### WORKFLOW SCHEDULING #####")
    (workflow_id, status, errors) = WorkflowManager.submit(data)

    return {
        "status" : status_name(status),
        "id": workflow_id,
        "errors": errors
    }

@route("cancel")
def cancel_workflow(data):
    '''Cancels the job if it is running.'''
    logger.info("##### WORKFLOW CANCELLATION #####")
    try:
        identity = data['id']
        status = WorkflowManager.cancel(identity)
        logger.info(status_message(identity, status))
        return {"status" : status_name(status)}
    except KeyError:
        return {"status" : 'NotFound'}

@route("workflows")
def get_workflows(data):
    '''Return all matching workflows'''
    ids = None

    if data:
        ids = data.get('ids', [])

    workflows = WorkflowManager.get_workflows(ids)
    result = []

    for (workflow_id, start, stop, status) in workflows:
        status_message = status_name(status)
        result.append((workflow_id, start, stop, status_message))

    return { "workflows" : result }

@route("get_status")
def get_workflow_status(data):
    '''Gets the status of the workflow.'''
    logger.info("##### WORKFLOW STATUS CHECK #####")
    try:
        identity = data['id']
        (status, jobs) = WorkflowManager.status(identity)
        logger.info(status_message(identity, status))
        return {"status" : status_name(status), "jobs" : jobs}
    except KeyError:
        return {"status" : 'NotFound', "jobs" : {}}
