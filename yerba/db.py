import json
import sqlite3

from time import time
from .core import Status

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

FIND_WORKFLOW_QUERY = '''
    SELECT * FROM workflows
    WHERE workflow=?
'''

GET_STATUS_QUERY = '''
    SELECT status FROM workflows
    WHERE id=?
'''

INSERT_WORKFLOW_QUERY = '''
    INSERT INTO workflows(workflow, submitted, completed, status)
    VALUES (?, ?, ?, ?)
'''

UPDATE_FIELD_QUERY = '''
    UPDATE workflows
    SET {field}=? WHERE id=?
'''

SERALIZE = json.JSONEncoder()

class Database(object):
    def __init__(self):
        self.handle = None

    def connect(self, filename):
        """
        Returns a connection to the database
        """
        self.handle = sqlite3.connect(filename)

    def execute(self, query, params=()):
        """
        Executes a query on the database
        """
        try:
            with self.handle:
                cursor = self.handle.execute(query, params)
            return cursor
        except sqlite3.IntegrityError:
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
    database = sqlite3.connect(filename)
    database.execute(CREATE_TABLE_QUERY)
    database.execute(START_INDEX_QUERY, (start_index,))
    database.close()

def get_status(database, workflow_id):
    """
    Returns the status of the workflow
    """

    cursor = database.execute(GET_STATUS_QUERY, (workflow_id,))
    row = cursor.fetchone()

    if row:
        return row[0]
    else:
        return Status.NotFound

def find_workflow(database, workflow):
    """
    Finds the workflow and returns its id
    """
    workflow_json = SERALIZE.encode(workflow)
    cursor = database.execute(FIND_WORKFLOW_QUERY, (workflow_json,))
    return cursor.fetchone()

def add_workflow(database, workflow):
    """
    Adds the workflow and returns its id
    """
    workflow_json = SERALIZE.encode(workflow)
    params = (workflow_json, time(), None, Status.Scheduled)
    database.execute(INSERT_WORKFLOW_QUERY, params)

def update_status(database, workflow_id, status):
    """
    Updates the status of the workflow
    """
    query = UPDATE_FIELD_QUERY.format(field="status")
    params = (status, workflow_id)
    return database.execute(query, params)
