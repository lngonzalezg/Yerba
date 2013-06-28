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

class ChainMap(UserDict.DictMixin):
    def __init__(self, *maps):
        self._maps = maps

    def __getitem__(self, key):
        for mapping in self._maps:
            with ignored(KeyError):
                item = mapping[key]
                return item

        raise KeyError(key)

class YerbaError(Exception):
    def __init__(self, msg):
        self._msg = msg
    def __str__(self):
        return repr(self._msg)
