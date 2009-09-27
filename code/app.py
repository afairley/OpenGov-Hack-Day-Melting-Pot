from webob import Request, Response
from webob.exc import *
from wsgiref.simple_server import make_server
import sys
import traceback
import routes

def hello(env, route, req, res):
    return 'Hello world'

def dispatch(env, req):
    route = routes.Mapper()
    route.environ = env
    route.connect(':action/:id', controller=hello)
    return route.match(req.path)

def app(env, start_response):

    req = Request(env) 
    res = Response()
    res.content_type = 'text/html'

    try:
        route = dispatch(env, req)
        handler = route['controller']
        if handler.__module__ == '__main__':
            module = __import__(__file__.rsplit('.', 1)[0])
        else:
            module = __import__(handler.__module__)
        handler = getattr(reload(module), handler.__name__)
        result = handler(env, route, req, res) 
        if result: res.body = result 
        return res(env, start_response)
    except HTTPException, e:
        return e(env, start_response)
    except:
        tb = traceback.format_exc()
        exc_value = sys.exc_info()[1]
        return Response(status=500, content_type='text/plain', body=tb)(env, start_response)

if __name__ == '__main__':
    server = make_server('', 9000, app)
    server.serve_forever()

