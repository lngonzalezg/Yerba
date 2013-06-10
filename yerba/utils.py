from contextlib import contextmanager
@contextmanager
def ignored(*exceptions):
    '''
    Ignores the set of exceptions passed to it
    '''
    try:
        yield
    except exceptions:
        pass
