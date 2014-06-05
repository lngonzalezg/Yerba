from json import JSONEncoder
from sqlite3 import connect, IntegrityError
from time import time

from yerba.core import Status

CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS workflows
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
     workflow BLOB,
     submitted TEXT,
     completed TEXT,
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

def get_status(database, workflow_id):
    """
    Returns the status of the workflow
    """
    query = '''
        SELECT status FROM workflows
        WHERE id=?
    '''

    cursor = database.execute(query, (workflow_id,))
    row = cursor.fetchone()

    if row:
        return row[0]
    else:
        return Status.NotFound

def find_workflow(database, workflow):
    """
    Finds the workflow and returns its id
    """
    query = '''
        SELECT * FROM workflows
        WHERE workflow=?
    '''
    workflow_json = encoder.encode(workflow['jobs'])
    cursor = database.execute(query, (workflow_json,))
    return cursor.fetchone()

def add_workflow(database, workflow_object=None, status=Status.Initialized):
    """
    Adds the workflow and returns its id
    """
    query = '''
        INSERT INTO workflows(workflow, submitted, completed, status)
        VALUES (?, ?, ?, ?)
    '''

    if workflow_object:
        jobs = workflow_object['jobs']
        params = (encoder.encode(jobs), time(), None, status)
    else:
        params = (None, time(), None, status)

    cursor = database.execute(query, params)
    return str(cursor.lastrowid)

def get_workflow(database, workflow_id):
    """
    Returns the pickled workflow from the database
    """

    query = """
        SELECT workflow
        FROM workflows
        WHERE workflow_id=?
    """
    cursor = database.execute(query, (workflow_id,))
    return cursor.fetchone()

def update_workflow(database, workflow_id, workflow):
    """
    Persists the pickled workflow into the database
    """
    query = """
        UPDATE workflows
        SET workflow=?
        WHERE id=?
    """
    params = (workflow, workflow_id)
    database.execute(query, params)

def update_status(database, workflow_id, status, completed=False):
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

    database.execute(query, params)

def get_workflows(database, ids=None):
    """
    Returns a subset of workflows

    If ids is specified the workflows will be limited to the subset of
    of workflows with matching ids.
    """

    if ids:
        query = '''
            SELECT id, submitted, completed, status
            FROM workflows WHERE id IN ({ids})
        '''

        id_string = ",".join(set(str(workflow_id) for workflow_id in ids))
        cursor = database.execute(query.format(ids=id_string))
    else:
        query = '''
            SELECT id, submitted, completed, status
            FROM workflows
        '''
        cursor = database.execute(query)

    return cursor.fetchall()
