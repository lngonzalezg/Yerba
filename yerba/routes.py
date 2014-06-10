from yerba import utils

ROUTES = {}

def route(request):
    '''Returns the request as a new endpoint.'''
    def callback(func):
        ROUTES[request] = func
    return callback

def dispatch(request):
    '''Dispatches request to given route'''
    with utils.ignored(KeyError):
        endpoint = request['request']
        data = request['data']
        return ROUTES[endpoint](data)

    raise RouteNotFound("The request could not be routed.")

class RouteNotFound(utils.YerbaError):
    '''Exception raised when a dispatch route is not found.'''
