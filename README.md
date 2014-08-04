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

### Configure startup scripts
The job engine comes with three init scripts *work_queue_pool*, *catalog_server*
 and *yerba*. These scripts should be placed into init.d
directory and setup to start at boot time.

```bash
sudo cp yerba work_queue_pool catalog_server /etc/init.d/
sudo update-rc.d yerba defaults
sudo update-rc.d catalog_server defaults
sudo update-rc.d work_queue_pool defaults
```
