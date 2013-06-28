import utils

_routes = {}

def route(request):
    '''Returns the request as a new endpoint.'''
    def callback(func):
        _routes[request] = func
    return callback

def dispatch(request):
    '''Dispatches request to given route'''
    with utils.ignored(KeyError):
        route = request['request']
        data = request['data']
        return _routes[route](data)

    raise RouteNotFound("The request could not be routed.")

class RouteNotFound(utils.YerbaError):
    '''Exception raised when a dispatch route is not found.'''
