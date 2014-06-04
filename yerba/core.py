from collections import namedtuple

_status_types = [
    "Initialized",
    "Scheduled",
    "Running",
    "Completed",
    "Cancelled",
    "Stopped",
    "Failed",
    "NotFound",
    "Error"
]

def status_name(code):
    return _status_types[code]

indices = range(len(_status_types))
Status = namedtuple('Status', ' '.join(_status_types))._make(indices)

_status_messages = {
    Status.Initialized: "The workflow {0} has been initalized",
    Status.Scheduled: "The workflow {0} has been scheduled.",
    Status.Running: "The workflow {0} is running.",
    Status.Completed: "The workflow {0} was completed.",
    Status.Cancelled: "The workflow {0} has been cancelled.",
    Status.Stopped: "The workflow {0} has been stopped.",
    Status.Failed: "The workflow {0} failed.",
    Status.NotFound: "The workflow {0} was not found.",
    Status.Error: "The workflow {0} has errors."
}

def status_message(name, code):
    return _status_messages[code].format(name)

