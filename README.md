Yerba
=====

CoGe's Yerba is a distributed job management framework.

Third-party Dependencies
------------------------

 * [pyzmq](https://pypi.python.org/pypi/pyzmq)
 * [cctools](http://www3.nd.edu/~ccl/software/download.shtml)

Installation
------------

Yerba has two main dependencies ZMQ and the cctools. The cctools contain the
python bindings to work_queue where Yerba sends work to be completed.
Additionally, work_queue_workers are required for any work to be completed
locally. The bin directory contains the daemon _yerbad_ and the client _yerba_.

### Setting up the Yerba Database
Before the job engine can be started the workflow database needs to be created.
The following command will generate a new database.

```bash
yerbad --config yerba.cfg --setup
```

### Install startup scripts and start job engine

The job engine comes with three upstart scripts which will setup yerba to restart when the system is rebooted.

```bash
cd scripts
sudo cp yerba.config work_queue_pool.config catalog_server.config /etc/init.d/
sudo start yerba
sudo start catalog_server
sudo start work_queue_pool
```

### Requests
This is the list of valid requests that can be submitted to Yerba.

##### Initialize a workflow
Initializes a new workflow and returns whether the creation was successful.

###### Request
```json
{
  "request": "new"
}
```
###### Response
```json
{
  "status": "<Status>",
  "id" : "<new workflow id>"
}
```
##### Create/Submit a workflow
Creates a new workflow if not present otherwise submits the workflow. Returns whether the workflow was successfully submitted.

Given an optional id will attempt to update an existing workflow with the workflow given.

###### Structure of a job
__description__ - The description used to describe the job in the get status response.

__cmd__ - The command that will be run by joining the cmd, script, and args into a command string.

__script__ - The script that is to be used as part of the command string to run the job.

__options__ - Optional arguments that change the behavior of how the workflow will complete.

__args__ - A list of triples where the third option is a flag to indicate whether the argument should attempt to be shortened.

__inputs__ - List of output dependencies that the job expects.

__outputs__ - List of input dependencies that the job requires.

__overwrite__ - A flag to indicate whether the job should be forced to be run.

###### Request
```json
{
  "request": "schedule",
  "data": {
    "name": "",
    "id": "<optional id>",
    "priority": "",
    "logfile": "",
    "jobs": ["<job1>", "<job_n>"]
  }
}
```
###### Response
```json
{
  "id": "<workflow id>",
  "status": "<Status>",
  "errors": []
}
```
##### Get Status
Returns the status of a workflow specified.

###### Request
```json
{
  "request": "get_status",
  "data": {
    "id": "<workflow_id>"
  }
}
```
###### Response
```json
{
  "status": "<Status>",
  "jobs": ["<job1>", "<job2>"]
}
```
##### Get Workflows
Returns the set of workflows who's ids are in the set of ids. If the set of ids is empty it will return all workflows. Each workflow returned will contain the __workflow_id, name, start time, stop time, status_message__

###### Request
```json
{
  "request": "workflows",
  "data": {
    "ids": ["<workflow_id_1>", "<workflow_id_2>"]
  }
}
```
###### Response
```json
{
  "workflows": ["<workflow_1>", "<workflow_2>"]
}
```

##### Restart workflow
Attempts to restart the workflow and returns the status.

###### Request
```json
{
  "request": "restart",
  "data": {
    "id": "<workflow_id>"
  }
}
```
###### Response
```json
{
  "status": "<Status>"
}
```

##### Cancel workflow
Attempts to cancel the workflow and returns the status.

###### Request
```json
{
  "request": "cancel",
  "data": {
    "id": "<workflow_id>"
  }
}
```
###### Response
```json
{
  "status": "<Status>"
}
```
