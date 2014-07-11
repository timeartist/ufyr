#stdlib
import os
import logging
from atexit import register
from random import triangular
from time import sleep


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
    
    if 'connection_pool' in kwargs:
        pass
    else:
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
        
    if args and callable(args[0]):
        f = args[0]
        limit = 1
        return _rate_limited(f)
    else:
        if args:
            limit = args[0]
        else:
            limit = 1
        return _rate_limited
   
   
def retry(*args, **kwargs):
    '''
    Simple exception try/catch surpress until a threshold is reached. 
    
    Each retry increases the delay of the next attempt, simply try_count * interval.
    
    Default behavior: Function must return something that resolves to bool(result) == True unless
    exc_only is defined.
    
    Options:
        limit - int - number of times to retry (default: 15)
        interval - tuple of ints - interval range to retry in (default: 3-5s)
        exc_only - bool - Only retry if the function raises an exception (Default: False)
        
    Examples:
        @retry
        def erratically_failing_function():
            return True or False
            
        @retry(exc_only=True):
        def erratically_failing_function():
            raise Exception("I don't always fail, but when I do, I do so exceptionally")
            return None
    '''
    def _retry(f):
        def _fx(*args, **fkwargs):
            success = False
            i = 0
            while not success:
                
                if i > limit:
                    raise Exception('Retry Limit Exceeded: %s.%s - %s %s'%(f.__module__, f.__name__,
                                                                           args, fkwargs))
                
                sleep(i*triangular(*interval))
                
                try:
                    result = f(*args, **fkwargs)
                    
                    if exc_only:
                        success = True
                    else:
                        success = result
                        
                except:
                    logging.exception('%s.%s -%s %s raised an Exception -- Retrying: %s'%(f.__module__,
                                                                                          f.__name__,
                                                                                          args,
                                                                                          fkwargs,
                                                                                          i + 1 > limit))
                finally:
                    i += 1
                    
            return result
                    
                    
        return _fx

        
    limit = kwargs.get('limit', 15)
    interval = kwargs.get('interval', (3, 5))
    exc_only = kwargs.get('exc_only', False)
    
    if args and callable(args[0]):
        return _retry(args[0])
    else:
        return _retry
                    
                
        
    