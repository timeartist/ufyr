import os
from time import sleep
from redis import StrictRedis, WatchError
from uuid import uuid4 as UUID
from atexit import register


DEFAULT_HOST = os.environ.get('REDIS_HOST', 'redis-db0')
DEFAULT_DB = os.environ.get('REDIS_DB', 1)


class RedisSemaphore():
    '''
    A basic semaphore that works across multiple servers/applications depending on how you use it.
    
    Usage:

    - Initialize an acquired semaphore with a limit of 1 on a localhost Redis instance.
    
        with RedisSemaphore(__name__):
            do stuff
    
    - Create a semaphore that allows for more than one lock:
    
        with RedisSemaphore(__name__, limit=N):
            do stuff

    - Create a semaphore using a remote Redis server or modify any other aspects of
      the semaphore's Redis connection.  The kwargs value is passed directly to the Redis
      creation object.
      
        with RedisSemaphore(__name__,**{'host':REDIS_HOST, 'db':REDIS_DB}):
            do stuff
        
    Working outside the with statement requires a little bit more understanding
    of the object itself.
    
        s = RedisSemaphore(__name__)
        
    Alone, `s` is our semaphore on localhost with a limit of 1 that is already acquired,
    not really useful when you're using it as a global.  If you want to utilize it that way
    I'd recommend the following pattern:
    
        RS = RedisSemaphore(__name__, limit=N, acquire=False, **{'host':REDIS_HOST, 'db':REDIS_DB})
        
    Then, you can acquire and release the lock as needed:
        
        RS.acquire()
        RS.release()
    
    Intialization Options:
        name - Name of the semaphore.
        limit - limit of locks available, defaults to 1
        acquire - if we should acquire upon intialization - defaults to True
        **kwargs -- Passed directly to Redis conn - allows for control of underlying Redis instance.
    '''
    
    
    def __init__(self, name, limit=1, acquire=True, **kwargs):
        
        if 'host' not in kwargs:
            kwargs['host'] = DEFAULT_HOST
        if 'db' not in kwargs:
            kwargs['db'] = DEFAULT_DB
            
        self.r = StrictRedis(**kwargs)
        self.limit = limit
        self.queue_members = []
        self.counter = 0

        self.key = 'semaphore.%s'%str(name)
        self.queue = 'semaphore.%s.queue'%str(name)
                        
        if acquire:
            self.acquire()
            
        register(self.__del__)
    
    
    def __enter__(self):
        return self
    
    
    def __exit__(self, *args):
        for queue_member in self.queue_members:
            self.r.lrem(self.queue, 0, queue_member)
        while self.counter > 0:
            self.release()
            self.counter -= 1
     
        
    def __del__(self):
        self.__exit__()
        val = int(self.r.get(self.key) or 0)
        if val == 0:
            self.r.delete(self.key)
    
        
    def _close_gracefully(self):
        self.__del__()
    
    
    def acquire(self, blocking=True):
        '''
        Acquire the semaphore lock, return True when acquired.
        if blocking (default) block operation until the lock becomes available;
        otherwise return False
        '''
        try:
            with self.r.pipeline() as pipe:
                
                pipe.watch(self.key)
                val = int(pipe.get(self.key) or 0)
                if val < self.limit:
                    pipe.multi()
                    pipe.incr(self.key)
                    pipe.execute()
                    self.counter += 1
                    return True
            
                elif blocking:
                    ##Wait in line until it's our turn - checking to see if it is every 1/5 of a second
                    queue_member = str(UUID())
                    self.r.rpush(self.queue, queue_member)
                    self.queue_members.append(queue_member)
                    
                    while True:
                        pipe.watch(self.key)
                        val = int(pipe.get(self.key) or 0)
                        if val < self.limit and pipe.lindex(self.queue, 0) == queue_member:
                            break
                        else:
                            pipe.unwatch()
                            sleep(.2)
                    
                    pipe.multi()
                    pipe.incr(self.key)
                    pipe.lpop(self.queue)
                    pipe.execute()
                    self.counter += 1
                    
                    self.queue_members.remove(queue_member)
                    
                    return True
            
                else:
                    return False
        except WatchError:
            self.acquire(blocking)
                
        finally:
            self.r.expire(self.key, 60*60*24)
        
    def release(self):
        '''
        Release the semaphore lock.
        
        Raises ValueError if there wasn't a lock available to release.
        '''
        try:
            new_val = self.r.decr(self.key)
            self.counter -= 1
            if new_val < 0:
                self.r.incr(self.key)
                raise ValueError, 'Semaphore lock must be acquired before it can be released.'
        finally:
            self.r.expire(self.key, 60*60)



        
    
    
    