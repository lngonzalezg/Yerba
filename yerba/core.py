from logging import getLogger
from collections import namedtuple

logger = getLogger('yerba.services')

_status_types = [
    "NotFound",
    "Waiting",
    "Running",
    "Completed",
    "Failed",
    "Terminated",
    "Scheduled",
    "Attached",
    "Error"
]

def status_name(code):
    return _status_types[code]

Status = namedtuple('Status', ' '.join(_status_types))._make(range(0, 9))

_status_messages = {
    Status.Attached: "The workflow {0} is Attached",
    Status.Scheduled: "The workflow {0} has been scheduled.",
    Status.Completed: "The workflow {0} was completed.",
    Status.Terminated: "The workflow {0} has been terminated.",
    Status.Failed: "The workflow {0} failed.",
    Status.Error: "The workflow {0} has errors.",
    Status.NotFound: "The workflow {0} was not found.",
    Status.Running: "The workflow {0} is running."
}

def status_message(name, code):
    return _status_messages[code].format(name)

