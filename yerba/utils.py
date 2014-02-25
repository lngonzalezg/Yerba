from contextlib import contextmanager
import UserDict

@contextmanager
def ignored(*exceptions):
    '''
    Ignores the set of exceptions passed to it
    '''
    try:
        yield
    except exceptions:
        pass

def meminfo():
    with open("/proc/meminfo") as fp:
        return dict([[item.strip() for item in line.rstrip("\n").split(":")]
            for line in fp])

class ChainMap(UserDict.DictMixin):
    def __init__(self, *maps):
        self._maps = maps

    def __getitem__(self, key):
        for mapping in self._maps:
            with ignored(KeyError):
                item = mapping[key]
                return item

        raise KeyError(key)

    def __setitem__(self, key, value):
        self._maps[0][key] = value

class YerbaError(Exception):
    def __init__(self, msg):
        self._msg = msg
    def __str__(self):
        return repr(self._msg)
