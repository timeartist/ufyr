#stdlib
import os
from atexit import register

#local
from semaphore import RedisSemaphore

def rate_limited(*args, **kwargs):
    '''
    Semaphore decorator.
    
    @rate_limited
    def foo():
        pass
        
    Will only allow one instance of foo to operate at one time - across all servers
    running your code.
    
    You can increase the limit easily:
    
    @rate_limited(5)
    def five_foo():
        pass
        
    Maintains locks on redis-db0/1 unless host/db are overwritten in kwargs
    '''
    
    if 'host' not in kwargs:
        kwargs['host'] = os.environ.get('REDIS_HOST', 'localhost')
    if 'db' not in kwargs:
        kwargs['db'] = os.environ.get('REDIS_DB', 0)

    def _rate_limited(f):
        name = '.'.join((f.__module__, f.__name__))
        s = RedisSemaphore(name, limit, acquire=False, **kwargs)
        
        @register
        def exit():
            s._close_gracefully()
        
        def fx(*args, **fkwargs):
            s.acquire()
 
            try:
                ret = f(*args, **fkwargs)
            finally:
                s.release()

            return ret
    
        return fx
        
    if callable(args[0]):
        f = args[0]
        limit = 1
        return _rate_limited(f)
    else:
        limit = args[0]
        return _rate_limited
   