# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
import random
import sys
from datetime import datetime
from functools import wraps
import traceback
from django.core.exceptions import ImproperlyConfigured
from .models import Job
from .utils import get_cache_queue




class CacheQ(object):
    """
    Manages Job queueing and queue messaging.
    
    Args
    
        using:      Cache backend to use with queue, as set in settings.CACHES 
                    (e.g., 'default', 'worker').
        name:       Queue name, which will identify the queue with the worker.
    
    Usage
    
    >>> cq = CacheQ(name='fast')
    >>> job = cq.enqueue(operator.add, 1,2)
    >>> job.ready()
    False
    >>> job.ready()
    True
    >>> job.result
    3
    """
    def __init__(self, name='default', using=''):
        self.queue = get_cache_queue(name=name, using=using)
        # add self.job decorator
        self.job = _jobdecorator(cacheq=self)
    
    def enqueue_many(self, tasks):
        """
        Add several tasks to a *single* job and message queue.
        """
        job = Job.objects.create()
        for task in tasks:
            job.add_task(task[0], *task[1], **task[2])
        self.queue.add_message(job.uuid)
        return job
    
    def enqueue(self, func, *args, **kwargs):
        """
        Add one task as a job and message queue.
        """
        job = Job.objects.create()
        job.add_task(func, *args, **kwargs)
        self.queue.add_message(job.uuid)
        return job


class _jobdecorator(object):
    """
    A decorator that adds a .delay() method decorated function.
    
    Usage:
    
    @job
    def mymethod(a,b):
        return a+b
    ...
    > job = mymethod.delay(1,2)  # delay returns corresponding job object
    """
    def __init__(self, cacheq):
        self.cacheq = cacheq
    
    def __call__(self, f):
        @wraps(f)
        def inner(*args, **kwargs):
            return f(*args, **kwargs)
        # add method to inner
        def delay(*args, **kwargs):
            return self.cacheq.enqueue(f, *args, **kwargs)
        inner.delay = delay
        return inner


class Worker(object):
    """
    A worker constantly polls the queue until it's messaged about a new job, which then 
    proceeds to run and store it's result in Job.result.
    
    Args
    
        cacheq:    A CacheQ instance.
        pulse:      Idle time to time.sleep(), in seconds.
        name:       Worker name to be displayed in log.
    
    Usage
    
    >>> cq = CacheQ(name='urgent')
    >>> worker = Worker(cq)
    >>> worker.run(burst=True, verbose=True)  # you can also run the worker in burst mode.
    """
    def __init__(self, cacheq, pulse=1, worker_name='worker'):
        self.cacheq = cacheq
        self.pulse = max(pulse, 1)
        self.worker_name = worker_name
    
    def _print(self, message, level='INFO'):
        """
        Adds datetime.now and worker name to output.
        """
        prefix = "[%s %s %s]:" % (self.worker_name, str(datetime.now()), level)
        print(" ".join([prefix, message]))
        # force print to stdout
        sys.stdout.flush()
    
    def _runjob(self, job):
        """
        Runs tasks in job.
        """
        results = []
        start = time.time()
        while True:
            task = job.pop_task()
            if not task:
                break
            func, args, kwargs = task
            results.append(func(*args, **kwargs))
        job.result = results if len(results) > 1 else results[0]
        job.execution_time = time.time() - start
        job.save()
        return True
    
    def _runburst(self, verbose):
        """
        Runs jobs in burst mode.
        """
        uuids = []
        # first let's clear cache queue
        if verbose:
            self._print("Clearing jobs in cache queue and checking database for jobs...")
        self.cacheq.queue.reset()
        jobs = Job.objects.filter(status=Job.PENDING)
        if (not jobs) and verbose:
            self._print("No pending jobs in databse.")
            return
        if verbose:
            self._print("Found %d pending jobs, proceeding to process..." % jobs.count())
        for job in jobs:
            job.running = True
            job.save()
            try:
                success = self._runjob(job)
            except:
                job.traceback = traceback.format_exc()
                success = False
            job.running = False
            job.status = Job.DONE if success else Job.FAILED
            job.save()
        if verbose:
            self._print("Done.")
    
    def _runloop(self):
        """
        Runs a loop that polls queue for new messages and runs jobs accordingly.
        """
        # when running parallel workers, we need to make sure they do not sync
        # because if they do, both will try to hit cache at the same time.
        # this will be a problem when testing, so we'll use a random pulse
        # for each loop.
        self._print("Worker ready...")
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 10
        while True:
            try:
                self._loop()
                consecutive_errors
            except ImproperlyConfigured:
                # TODO: log
                self._print("CacheQ improperly configured, shutting down.", "ERROR")
                print(traceback.format_exc())
                break
            except KeyboardInterrupt as e:
                print("")
                self._print("Shutting down...")
                break
            except:
                # TODO: log
                consecutive_errors += 1
                print(traceback.format_exc())
                if consecutive_errors < MAX_CONSECUTIVE_ERRORS:
                    # we can afford more errors
                    self._print("Could not complete loop properly, moving on...", "ERROR")
                self._print("Too many consecutive failed loops, shutting down.", "ERROR")
                break
    
    def _loop(self):
        """
        Executes the loop.
        """
        # pulse will be set to 0 for values under 0.1
        loop_pulse = 0 if self.pulse < 0.1 else random.uniform(0.1, self.pulse)
        job_uuid = self.cacheq.queue.pop_message()
        # we can get either a uuid string, None or False
        # getting None means there are no more jobs in queue
        # False means the lock is taken
        if job_uuid in [None, False]:
            # we need to wait in both cases
            time.sleep(loop_pulse)
            return
        # else we got a uuid string, so we can fetch a job
        # note that we could try to fetch a deleted job, so
        # we must catch exception
        self._print("Job.uuid = %s" % job_uuid)
        try:
            job = Job.objects.get(uuid=job_uuid)
        except Job.DoesNotExist:
            # job is outdated, we move on
            self._print("Job outdated, discarding...")
            time.sleep(loop_pulse)
            return
        # we proceed to run job
        job.running = True
        job.save()
        try:
            success = self._runjob(job)
        except:
            self._print("Execution error, saving job with status FAILED.")
            job.traceback = traceback.format_exc()
            self._print(job.traceback, "DEBUG")
            success = False
        job.running = False
        job.status = Job.DONE if success else Job.FAILED
        job.save()
        if success:
            self._print("Success, saving job with status DONE.")
        self._print("Waiting for next job...")
        time.sleep(loop_pulse)
    
    def run(self, burst=False, verbose=False):
        """
        Routes job processing to either _runburst or _runloop. Verbose argument only 
        works in burst mode.
        """
        if burst:
            return self._runburst(verbose)
        return self._runloop()


def get_worker(queue_name='default', using='default', worker_name='worker', pulse=1):
    """
    Returns a worker with cacheq = CacheQ(queue_name, using).
    """
    cacheq = CacheQ(name=queue_name, using=using)
    return Worker(cacheq=cacheq, pulse=pulse, worker_name=worker_name)
