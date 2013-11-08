#stdlib
import os
from atexit import register
from random import triangular

#3rd party
from gevent import sleep 

#local
from semaphore import RedisSemaphore
from logger import Logger

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
   
   
def retry(*args, **kwargs):
    def _retry(f):
        def _fx(*args, **fkwargs):
            success = False
            i = 0
            while not success:
                
                sleep(i*triangular(*interval))
                
                if i > limit:
                    raise Exception('Retry Limit Exceeded: %s.%s - %s %s'%(f.__module__, f.__Name__,
                                                                           args, fkwargs))
                try:
                    success = f(*args, **fkwargs)
                except:
                    import traceback
                    traceback.print_exc()
                finally:
                    i += 1
                    
                    
        return _fx

        
    limit = kwargs.get('limit', 15)
    interval = kwargs.get('interval', (3, 5))
    
    if args and callable(args[0]):
        return _retry(args[0])
    else:
        return _retry
                    
                
        
    