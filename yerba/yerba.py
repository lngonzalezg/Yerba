import argparse
import heapq
import json as js
import logging
import logging.handlers as loghandlers
import itertools
import os
import time

import shelve
import zmq

from makeflow import MakeflowService
from managers import (ServiceManager)

REQUEST = 'request'
DATA = 'data'

ERROR = "-1"

workflows = {}
jex_queue = []
counter = itertools.count()
encoder = js.JSONEncoder()

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

def schedule_workflow(data):
    """ Returns the job id"""
    workflow_service = ServiceManager.get("makeflow", "workflow")
    status_service = ServiceManager.get("status", "internal")
    new_workflow = workflow_service.create_workflow(data)

    status_code = status_service.SCHEDULED

    if not new_workflow:
        status_code = status_service.FAILED
    elif new_workflow.name in workflows:
        status_code = status_service.ATTACHED
    else:
        workflows[new_workflow.name] = new_workflow

        # Add the workflow to the queue
        count = next(counter)
        entry = (new_workflow.priority, count, new_workflow.name)
        heapq.heappush(jex_queue, entry)

        # Persist the job to the shelve
        #try:
        #    workflow_database = shelve.open("yerba")
        #    workflow_database[new_workflow.name] = new_workflow
        #    workflow_database.close()
        #except:
        #    logging.exception("Unable store workflow in shelve.")

    return status_code

def fetch_workflow():
    """ Fetches the job for the given workflow_key"""
    (priority, count, workflow_key) = heapq.heappop(jex_queue)

    return workflows[workflow_key]

def get_status(workflow_key):
    status_service = ServiceManager.get("status", "internal")

    if workflow_key in workflows:
        workflow = workflows[workflow_key]

        workflow_service = ServiceManager.get("makeflow", "workflow")
        status_code = workflow_service.get_status(workflow)
        status_message = status_service.status_message(status_code)

        if status_code == status_service.COMPLETED:
            del workflows[workflow_key]

            #try:
            #    workflow_database = shelve.open("yerba", writeback=True)
            #    if workflow_key in workflow_database:
            #        del workflow_database[workflow_key]
            #    workflow_database.close()
            #except:
            #    logger.exception("Could not open shelve for writing")

        logger.info("%s is %s", workflow, status_message)
    else:
        status_code = status_service.NOT_FOUND
        status_message = status_service.status_message(status_code)

    return status_code

def dispatch(request, data):
    if request == "get_status":
        status_code = get_status(data)
    elif request == "schedule":
        status_code = schedule_workflow(data)

    return encoder.encode({"status" : status_code})

def listen_forever(port):
    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)

    jd = js.JSONDecoder()
    status_service = ServiceManager.get("status", "internal")
    while True:
        request = socket.recv()

        req_object = None
        response = None

        try:
            request_string = request.decode("ascii")
            req_object = jd.decode(request_string)
        except:
            logger.exception("Request was not able to be decoded.")

        if req_object and REQUEST in req_object and DATA in req_object:
            response = dispatch(req_object['request'], req_object['data'])
        else:
            response = encoder.encode({"status" : status_service.ERROR})

        socket.send_unicode(response)

        if jex_queue:
            logger.info("Fetching new workflow to run.")
            workflow_service = ServiceManager.get("makeflow", group="workflow")
            workflow_service.run_workflow(fetch_workflow())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Processes bioinformatic jobs.')

    parser.add_argument('port')
    parser.add_argument('--log')
    args = parser.parse_args()

    if args.log:
        log_level = getattr(logging, args.log.upper(), None)

        if not isinstance(log_level, int):
            raise ValueError('Invalid log level: %s' % log_level)
        logger.setLevel(log_level)

    # Register services
    ServiceManager.initialize()
    ServiceManager.start()
    ServiceManager.register(MakeflowService())

    listen_forever(args.port)
