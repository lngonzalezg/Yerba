import argparse
import heapq
import json as js
import logging
import itertools
import os
import subprocess
import sys
import time
import uuid

import zmq

from makeflow import (JobBuilder, MakeflowBuilder)

REQUEST = 'request'
DATA = 'data'

ERROR = "-1"

workflows = {}
jex_queue = []
counter = itertools.count()
encoder = js.JSONEncoder()

# Setup Logging
logger = logging.getLogger('yerba')
logger.setLevel(logging.DEBUG)

fmt = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S')

filehandler = logging.FileHandler('yerba.log')
filehandler.setLevel(logging.DEBUG)
filehandler.setFormatter(fmt)

streamhandler = logging.StreamHandler()
streamhandler.setLevel(logging.INFO)
streamhandler.setFormatter(fmt)

logger.addHandler(filehandler)
logger.addHandler(streamhandler)

def build_workflow(workflow, id):
    wb = MakeflowBuilder("%s-%s" % (workflow['name'], id))
    jobs = workflow['jobs']

    for job in jobs:
        jb = JobBuilder(job['cmd'])
        if 'inputs' in job:
            jb.add_file_group(job['inputs'])

        if 'outputs' in job:
            jb.add_file_group(job['outputs'], remote=True)

        wb.add_job(jb.build_job())

    wb.build_workflow()
    return "%s-%s.makeflow" % (workflow['name'], id)

def schedule_workflow(workflow):
    id = str(uuid.uuid4().int)
    makeflow = build_workflow(workflow, id)
    count = next(counter)

    entry = (0, count, [id, makeflow])
    workflows[id] = entry
    heapq.heappush(jex_queue, entry)
    print(jex_queue)

    return id
    
def run_workflow():
    (priority, count, workflow) = heapq.heappop(jex_queue)
    (id, makeflow) = workflow

    print("Running workflow %s" % workflow)

    try:
        print(makeflow)
        os.popen("makeflow -T wq -N coge -a -C localhost:1024 %s &" %
                os.path.abspath(makeflow))
    except Exception as e:
        print ("error: %s" % e)
        logger.warn("Unable to run workflow.")

def get_status(id):
    if id in workflows:
        (priority, count, workflow) = workflows[id]
        (id, makeflow) = workflow
        
        try:
            lines = open("%s.makeflowlog" % makeflow).readlines()
        except Exception as e:
            print("Unable to open makeflow %s" % e)
            return encoder.encode({ "status" : "RUNNING"})

        for line in lines:
            if line.startswith("#"):
                status = line.split()[1]

        if status:
            logger.info(status)
            return encoder.encode({ "status" : status})
        else:
            return encoder.encode({ "status" : "RUNNING"})
    else:
        return encoder.encode({ "status" : "NOT_FOUND"})
  
def dispatch(request, data):
    if request == "get_status":
        return get_status(data)
    elif request == "schedule":
        return schedule_workflow(data)

def listen_forever(port):
    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)

    jd = js.JSONDecoder()

    while True:
        request = socket.recv()

        try:
            req_object = jd.decode(request)
        except:
            logger.warn("Request was not able to be decoded.")
        
        if req_object and REQUEST in req_object and DATA in req_object:
            response = dispatch(req_object['request'], req_object['data'])

        if response:
            socket.send(response)
        else:
            socket.send(ERROR)
        
        if jex_queue:
            run_workflow()

        time.sleep(1)


class WorkflowService():
    def __init__(self):
        pass

    def get_status(self, workflow):
        ''' Get the current status of the workflow.'''
        pass

    def create_workflow(self, workflow):
        ''' Given a JSON workflow format it will construct the workflow.'''
        pass
    
    def run_workflow(self, workflow):
        ''' Executes a given workflow.'''
        pass


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
    
    listen_forever(args.port)
