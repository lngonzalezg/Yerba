Yerba
=====

CoGe's Yerba is a distributed job management framework.

Dependencies
------------

 * pyzmq
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
