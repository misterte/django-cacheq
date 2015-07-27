#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_django-cacheq
------------

Tests for `django-cacheq` module.
"""
import os
import shutil
from unittest import TestCase
import sys
import operator
import tempfile
from contextlib import wraps
from django.test import TestCase
from django.contrib.auth.models import User
from django.core.cache import get_cache
from django.core.cache.backends.memcached import MemcachedCache
from django.conf import settings
from django.core.management import call_command
from cacheq.models import Job
from cacheq.utils import get_cache_queue
from cacheq.base import CacheQ, get_worker




class JobTests(TestCase):
    """
    Tests that we can delay tasks with cacheq.
    """
    def test_add_and_pop_tasks(self):
        """
        We can add a task by using Job.add_task and then pop it by using Job.pop_task. 
        Tasks are popped in the order they are added, and None is returned when there 
        are no more tasks available.
        """
        tasks = [
            (operator.add, (1, 2), {}),
            (operator.mul, (-1, 0.123), {}),
            (operator.div, (), {'a': 4, 'b': 2}),
            (int, ('1234',), {}),
        ]
        j = Job.objects.create()
        for task in tasks:
            j.add_task(task[0], *task[1], **task[2])
        for task in tasks:
            self.assertEqual(task, j.pop_task())
        self.assertEqual(None, j.pop_task())


class LockFileCacheQueueTests(TestCase):
    """
    Tests that we can manage a queue with CacheQueue.
    """
    def setUp(self):
        self.cq = get_cache_queue(name='LockFileCacheQueueTests')
        # we need to reset queue to avoid overlapping with other tests
        self.cq.reset()
    
    def test_add_message_pop_message(self):
        """
        Tests that we can add and retrieve messages in order.
        """
        messages = ['foo', 'bar', 123, {'spam': (0.123, True)}]
        for message in messages:
            self.cq.add_message(message)
        for message in messages:
            self.assertEqual(message, self.cq.pop_message())
    
    def test_lock_release(self):
        """
        Tests the use of .lock and .release methods.
        """
        self.assertTrue(self.cq.lock())
        self.assertFalse(self.cq.lock())
        self.cq.release()
        self.assertTrue(self.cq.lock())
        # lock is active, so we cannot add messages
        self.cq.add_message('foo')
        # when popping a message, if the lock is taken,
        # we will get False, not None. None means the 
        # queue is empty.
        self.assertEqual(self.cq.pop_message(), False)
        # and lock is still active
        self.assertFalse(self.cq.lock())
        # if we release it, we can add a message & read
        self.cq.release()
        self.cq.add_message('bar')
        self.assertEqual(self.cq.pop_message(), 'bar')
        # which will in turn be able to release the lock
        self.assertTrue(self.cq.lock())
        # let's release for other tests
        self.cq.release()
    
    def test_reset(self):
        """
        Tests that we can revese queue to it's initial state.
        """
        self.cq.add_message('spam')
        self.assertTrue(len(self.cq.registry.all()) == 1)
        message_key = self.cq.registry.all()[0]
        self.cq.lock()
        self.assertFalse(self.cq.lock())
        # now we reset queue
        self.cq.reset()
        # message should be gone from cache
        self.assertFalse(self.cq.client.get(message_key, False))
        # there should be no keys in registry
        self.assertEqual(len(self.cq.registry.all()), 0)
        # and lock should be gone
        self.assertTrue(self.cq.lock())
        # let's release lock for other tests
        self.cq.release()
    
    def tearDown(self):
        self.cq.release()


cqsettings = getattr(settings, 'CACHEQ', {})
using = cqsettings.get('MEMCACHED_TESTS_USING', 'default')
if not type(get_cache(using)) is MemcachedCache:
    print("""Warning: exsutils.MemcachedCacheQueueTests will be run as dummy, 
    as cache backend in not compatible.""")


class ConditionalBackendTester(object):
    """
    Overloads the __getattr__ method in order to avoid running tests if self.RUN_TESTS 
    is set to False. Note that this class is meant to be used with a proper cache queue 
    testing subclass.
    """
    def __getattr__(self, attrname):
        obj = super(ConditionalBackendTester, self).__getattr__(attrname)
        if not attrname.startswith('test'):
            return obj
        # obj is a test function, so we'll call 
        # it within a wrapper and return that result
        @wraps(obj)
        def inner(self, *args, **kwargs):
            # run tests conditionally
            if self.RUN_TESTS:
                return obj(self, *args, **kwargs)
        return inner


class MemcachedCacheQueueTests(LockFileCacheQueueTests, ConditionalBackendTester):
    """
    Runs same tests as in LockFileCacheQueueTests, but with memcached as backend. 
    These tests require memcached to be running. It will look for the cache backend 
    setting in settings.CACHEQ['MEMCACHED_TESTS_USING'] or check if current default 
    backend is MemcachedCache.
    """
    def setUp(self):
        # get alternate cache from settings
        _sqsettings = getattr(settings, 'CACHEQ', {})
        using = _sqsettings.get('MEMCACHED_TESTS_USING', 'default')
        self.cq = get_cache_queue(name='MemcachedCacheQueueTests', using=using)
        # reset queue to avoid overlapping with other test behavior
        self.cq.reset()
        self.RUN_TESTS = type(get_cache(using)) is MemcachedCache


class RedisCacheQueueTests(LockFileCacheQueueTests):
    """
    This is analogous to MemcachedCacheQueueTests, but for a redis backend.
    """
    def setUp(self):
        # get alternate cache from settings
        cqsettings = getattr(settings, 'CACHEQ', {})
        using = cqsettings.get('REDIS_TESTS_USING', 'default')
        self.cq = get_cache_queue(name='RedisCacheQueueTests', using=using)
        # reset queue to avoid overlapping with other test behavior
        self.cq.reset()
        self.RUN_TESTS = type(get_cache(using)).__name__ == 'RedisCache'


# create a testing queue and some decorated funcs
cq = CacheQ(name='CacheQTests')

@cq.job
def myfunc(a,b):
    return a+b

def task1(fname, content):
    with open(fname, 'w') as f:
        f.write(content)

def task2(fname, content):
    with open(fname, 'r') as f:
        c = f.read()
    assert c == content

@cq.job
def failtask():
    raise Exception("Custom exception")


class CacheQTests(TestCase):
    """
    Tests that we can create a CacheQ object that manages job placement.
    """
    def setUp(self):
        self.cq = CacheQ('CacheQTests')
    
    def test_enqueue(self):
        """
        Tests that we can enqueue jobs using CacheQ.enqueue.
        """
        self.cq.enqueue(operator.add, 1, 2)
        # check if a Job object was created
        self.assertEqual(Job.objects.count(), 1)
        job = Job.objects.all()[0]
        task = job.pop_task()
        self.assertEqual(operator.add, task[0])
        # JSON field won't store tuples, so we need 
        # to take that into account for args
        self.assertEqual([1, 2], task[1])
        self.assertEqual({}, task[2])
        self.assertEqual(job.status, Job.PENDING)
        self.assertEqual(job.running, False)
        # no more tasks in job
        self.assertEqual(job.pop_task(), None)
        # check if message queue was signaled
        self.assertEqual(self.cq.queue.pop_message(), job.uuid)
    
    def test_enqueue_many(self):
        """
        Tests that we can enqueu a list of tasks using the following syntax:
        
        sqobj.enqueue_many([(func1, args1, kwargs1), (func2, args2, kwargs2), ...])
        """
        # JSON field will not store tuples, only lists
        # so we need to take that into account for args
        tasks = [(operator.add, [1,2], {}), (operator.div, [], {'a': 2, 'b': 2})]
        self.cq.enqueue_many(tasks)
        # only one job was created
        self.assertEqual(Job.objects.count(), 1)
        job = Job.objects.all()[0]
        # test tasks are equal
        for task in tasks:
            test_iter = zip(task, job.pop_task())
            for item in test_iter:
                self.assertEqual(*item)
        # no more tasks are available
        self.assertEqual(job.pop_task(), None)
        # check if message queue was signaled
        self.assertEqual(self.cq.queue.pop_message(), job.uuid)
    
    def test_job_decorator(self):
        """
        Tests that we can enqueu a function by calling func.delay(*args, **kwargs), which 
        will create a Job with one task and return the job instance.
        """
        decorated = self.cq.job(operator.div)
        decorated.delay(1,b=2)
        # this should have created one job
        self.assertEqual(Job.objects.count(), 1)
        job = Job.objects.all()[0]
        task = job.pop_task()
        self.assertEqual(task[0], operator.div)
        self.assertEqual(task[1], [1])
        self.assertEqual(task[2], {'b': 2})
        # no more tasks
        self.assertEqual(job.pop_task(), None)
        # check if message queue was signaled
        self.assertEqual(self.cq.queue.pop_message(), job.uuid)
        # try with decorated myfunc
        myfunc.delay(2,3)
        self.assertEqual(Job.objects.count(), 2)
        job = Job.objects.all()[1]
        task = job.pop_task()
        self.assertEqual(task[0], myfunc)
        self.assertEqual(task[1], [2,3])
        self.assertEqual(task[2], {})
        self.assertEqual(job.pop_task(), None)
        # check if message queue was signaled
        self.assertEqual(self.cq.queue.pop_message(), job.uuid)


class WorkerTests(TestCase):
    """
    Tests cacheq's worker. This worker will poll the message queue and run jobs accordingly, 
    also updating job status and results.
    """
    def setUp(self):
        self.cq = CacheQ(name='CacheQTests')
        self.worker = get_worker(queue_name='CacheQTests', worker_name='testworker')
        _, self.tempfname = tempfile.mkstemp()
    
    def test_single_task(self):
        """
        Tests we can process a single task in burst mode.
        """
        job = self.cq.enqueue(operator.add, 1,2)
        self.worker.run(burst=True)
        # check job status and result
        self.assertTrue(job.ready())
        self.assertEqual(job.status, Job.DONE)
        self.assertEqual(job.result, 3)
        self.assertEqual(type(job.execution_time), float)
        self.assertTrue(job.execution_time > 0)
    
    def test_multiple_tasks(self):
        """
        Tests we can process multiple tasks for a single job in burst mode.
        """
        job = self.cq.enqueue_many([
                (operator.add, (2,3), {}), (operator.div, (5,float(2)), {})])
        self.worker.run(burst=True)
        # check job status and results
        self.assertTrue(job.ready())
        self.assertEqual(job.status, Job.DONE)
        self.assertEqual(job.result, [5, 2.5])
    
    def test_complex_tasks(self):
        """
        Tests that we can add a series of tasks that depend on each other, which will be run 
        in order. Also, multiple jobs will be submitted.
        """
        job1 = self.cq.enqueue_many([
            (task1, (self.tempfname, 'Hello world!'), {}), 
            (task2, (self.tempfname, 'Hello world!'), {})])
        job2 = self.cq.enqueue(operator.add, 3,3)
        self.worker.run(burst=True)
        # check job status and response
        self.assertTrue(job1.ready())
        self.assertEqual(job1.status, Job.DONE)
        self.assertEqual(job1.result, [None, None])
        self.assertTrue(job2.ready())
        self.assertEqual(job2.status, Job.DONE)
        self.assertEqual(job2.result, 6)
    
    def test_exception(self):
        """
        Tests that jobs can handle exceptions properly.
        """
        job = failtask.delay()
        self.worker.run(burst=True)
        # let's check status and traceback
        self.assertTrue(job.ready())
        self.assertEqual(job.status, Job.FAILED)
        self.assertEqual(job.result, None)
        self.assertTrue('raise Exception("Custom exception")' in job.traceback)
        self.assertFalse(job.running)
    
    def tearDown(self):
        # remove tempfile
        os.remove(self.tempfname)


class CommandsTests(TestCase):
    """
    Tests that we can both run worker and clear jobs using custom commands.
    """
    def setUp(self):
        self.cq = CacheQ(name='CacheQTests')
    
    def test_run_worker(self):
        """
        Tests that we can run a worker. We will test only with burst mode option.
        """
        self.cq.enqueue(operator.add, 1,2)
        self.assertEqual(Job.objects.filter(status=Job.PENDING).count(), 1)
        call_command('cqworker', burst=True, queue_name='CacheQTests')
        self.assertEqual(Job.objects.filter(status=Job.DONE).count(), 1)
    
    def test_clear_jobs(self):
        """
        Tests that we can clear jobs depending on status.
        """
        for status in [Job.PENDING, Job.PENDING, Job.FAILED, Job.FAILED, Job.DONE, Job.DONE]:
            Job.objects.create(status=status)
        self.assertEqual(Job.objects.count(), 6)
        call_command('cqclear', 'failed')
        self.assertEqual(Job.objects.count(), 4)
        self.assertFalse(Job.objects.filter(status=Job.FAILED).exists())
        call_command('cqclear', 'all', no_input=True)
        self.assertEqual(Job.objects.count(), 0)
