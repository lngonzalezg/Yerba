
class Service(object):
    def initialize(self):
        pass

class StatusService(Service):
    name = "status"
    group = "internal"

    status_codes = {
        -1: "NOT_FOUND",
        0: "WAITING",
        1: "RUNNING",
        2: "COMPLETED",
        3: "FAILED",
        4: "ABORTED",
        5: "STARTED",
        6: "SCHEDULED",
        7: "ATTACHED"
    }

    def status_code(self, name):
        codes = {value : key for (key, value) in self.status_codes.items()}

        if name in codes:
            return codes[name]
        else:
            return -1

    def add_status_code(self, code, name):
        if not code in self.status_codes:
            self.status_codes[code] = name

    def status_message(self, code):
        if code in self.status_codes:
            return self.status_codes[code]
        else:
            return self.status_codes[-1]

    def __getattr__(self, name):
        pos = name.find("_MESSAGE")
        key = name[0:pos]

        if name.endswith("_MESSAGE") and key in status_codes:
            attr = key
        else:
            attr = self.status_code(name)

        return attr
