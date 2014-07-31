from json import JSONEncoder
from sqlite3 import connect, IntegrityError
from time import time

from yerba.core import Status

CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS workflows
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
     name TEXT,
     log TEXT,
     jobs BLOB,
     submitted TEXT,
     completed TEXT,
     priority INTEGER,
     status INTEGER)
'''

START_INDEX_QUERY = '''
    UPDATE SQLITE_SEQUENCE
    SET seq=?
    WHERE name='workflows'
'''

encoder = JSONEncoder()

class Database(object):
    """
    A minimal interface that abstract the sqlite api
    """

    def __init__(self):
        self.handle = None

    def connect(self, filename):
        """
        Returns a connection to the database
        """
        self.handle = connect(filename)

    def execute(self, query, params=()):
        """
        Executes a query on the database
        """
        try:
            with self.handle:
                cursor = self.handle.execute(query, params)
            return cursor
        except IntegrityError:
            pass

    def close(self):
        """
        Closes the connect to the database
        """
        self.handle.close()


def setup(filename, start_index=0):
    """
    Creates the workflow table and reset the starting index
    """
    database = connect(filename)
    database.execute(CREATE_TABLE_QUERY)
    database.execute(START_INDEX_QUERY, (start_index,))
    database.close()

class WorkflowStore(object):
    def __init__(self, database):
        self.database = database

    def get_status(self, workflow_id):
        """
        Returns the status of the workflow
        """
        query = '''
            SELECT status FROM workflows
            WHERE id=?
        '''

        cursor = self.database.execute(query, (workflow_id,))
        row = cursor.fetchone()

        if row:
            return row[0]
        else:
            return Status.NotFound

    def find_workflow(self, jobs):
        """
        Finds the workflow and returns its id
        """
        query = '''
            SELECT * FROM workflows
            WHERE jobs=?
        '''
        jobs_json = encoder.encode(jobs)
        cursor = self.database.execute(query, (jobs_json,))
        return cursor.fetchone()

    def add_workflow(self, name=None, log=None, jobs=None,
                    priority=0, status=Status.Initialized):
        """
        Adds the workflow and returns its id
        """
        query = '''
            INSERT INTO workflows(name, log, jobs, submitted, completed,
                                status, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''

        if jobs:
            job_json = encoder.encode(jobs)
        else:
            job_json = None

        params = (name, log, job_json, time(), None, status, priority)

        cursor = self.database.execute(query, params)
        return cursor.lastrowid

    def get_workflow(self, workflow_id):
        """
        Returns the pickled workflow from the database
        """

        query = """
            SELECT *
            FROM workflows
            WHERE id=?
        """
        cursor = self.database.execute(query, (workflow_id,))
        return cursor.fetchone()

    def update_workflow(self, workflow_id, name=None, log=None, jobs=None,
                        priority=0):
        """
        Persists the pickled workflow into the database
        """
        query = """
            UPDATE workflows
            SET name=?, log=?, jobs=?, priority=?
            WHERE id=?
        """
        if jobs:
            job_json = encoder.encode(jobs)
        else:
            job_json = None

        params = (name, log, job_json, priority, workflow_id)
        self.database.execute(query, params)

    def update_status(self, workflow_id, status, completed=False):
        """
        Updates the status of the workflow
        """

        if completed:
            query = '''
                UPDATE workflows
                SET status=?, completed=? WHERE id=?
            '''
            params = (status, time(), workflow_id)
        else:
            query = '''
                UPDATE workflows
                SET status=? WHERE id=?
            '''
            params = (status, workflow_id)

        self.database.execute(query, params)

    def stop_workflows(self):
        """
        Set the status of all Running jobs to stopped
        """

        query = '''
            UPDATE workflows
            SET status=?, completed=?
            WHERE status=?
        '''

        params = (Status.Stopped, time(), Status.Running)
        self.database.execute(query, params)


    def fetch(self, ids=None):
        """
        Returns a subset of workflows

        If ids is specified the workflows will be limited to the subset of
        of workflows with matching ids.
        """

        if ids:
            query = '''
                SELECT id, name, submitted, completed, status, priority
                FROM workflows WHERE id IN ({ids})
            '''

            id_string = ",".join(set(str(workflow_id) for workflow_id in ids))
            cursor = self.database.execute(query.format(ids=id_string))
        else:
            query = '''
                SELECT id, name, submitted, completed, status, priority
                FROM workflows
            '''
            cursor = self.database.execute(query)

        return cursor.fetchall()
