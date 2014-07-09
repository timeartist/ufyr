from datetime import datetime, timedelta
from time import sleep, mktime

from rq import Queue, Connection, get_current_job
from redis import Redis

from semaphore import RedisSemaphore



def queue_job_with_countdown(func, args, queue, countdown, connection=None, requeue_count=0):
    
    job = get_current_job()
    q = Queue(job.origin, connection=connection) if connection else Queue(job.origin)
    qx = Queue(queue, connection=connection) if connection else Queue(queue)
    eta = job.enqueued_at + timedelta(seconds=countdown)
    
    while True:
        
        sleep(requeue_count/2 or 1)
        #print 'Hey Listen!'
        with RedisSemaphore('countdown'):
            #print 'Use the force Luke'
            if  mktime(datetime.utcnow().timetuple()) >= mktime(eta.timetuple()):
                #print 'Firing off Task'
                qx.enqueue(func, *args)
                return
            elif q.is_empty():
                #print 'Parent queue empty, sleeping'
                continue
            else:
                #print 'Parent queue has job, requeuing self to work that'
                time_elapsed = mktime(datetime.utcnow().timetuple()) - mktime(job.enqueued_at.timetuple())
                new_countdown = countdown - time_elapsed
                q.enqueue_call(func=queue_job_with_countdown,
                               args=(func, #delayed func
                                     args, #delayed func args
                                     queue,           #queue to deploy func to after delay
                                     new_countdown),
                               kwargs={'requeue_count':requeue_count + 1},
                               timeout=-1,
                               result_ttl=0)
                return