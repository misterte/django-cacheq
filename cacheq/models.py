# -*- coding: utf-8 -*-
import uuid
import importlib
from django.db import models
from django.dispatch.dispatcher import receiver
from django.db.models.signals import post_delete
from django.conf import settings
from django.core.cache import get_cache
from jsonfield import JSONField



#####################
# Background tasks
#####################
class Job(models.Model):
    """
    A job represents a series of tasks that must be executed by a background process.
    Tasks will be contained inside the 'tasks' field, which is a JSONField.
    
    For instance:
    >>> from exsutils.models import Job
    >>> import operator
    >>> job = Job.objects.create()
    >>> job.add_task(operator.add, 1,2)
    >>> job.tasks
    [{'args': (1, 2), 'name': 'add', 'module': 'operator', 'kwargs': {}}]
    
    Jobs will be identified by worker with a unique uuid. Tasks will be executed according 
    to job.tasks.pop(0).
    
    Note that Jobs sould be submitted via the exsutils.cacheq.CacheQ Api, *not* the 
    Job model, which is meant to be a low level Api.
    
    >>> import operator
    >>> from django.core.cache import cache
    >>> from exsutils.cacheq import CacheQ, Worker
    >>> from exsutils.models import Job
    >>> queue = CacheQ(name='testqueue', using='default') # as in settings.CACHES['default']
    >>> worker = Worker(cacheq=queue, worker_name='my-worker')
    >>> # or you can just use get_cacheq('testqueue') and get_worker('testqueue')
    >>> job = queue.enqueue(operator.add, 1,2)
    >>> worker.run(burst=True)  # note that burst mode is blocking
    >>> type(job) is Job
    True
    >>> job.ready()
    True
    >>> job.result
    3
    >>> job.execution_time > 0
    True
    """
    DONE, FAILED, PENDING = 'DONE', 'FAILED', 'PENDING'
    STATUS_CHOICES = (
        (DONE, "Done"),
        (FAILED, "Failed"),
        (PENDING, "Pending"), 
    )
    datetime_created = models.DateTimeField(blank=True, auto_now_add=True)
    datetime_last_updated = models.DateTimeField(blank=True, null=True, auto_now=True)
    running = models.BooleanField(default=False)
    status = models.CharField(choices=STATUS_CHOICES, default=PENDING, max_length=100)
    uuid = models.CharField(default=lambda *args, **kwargs: str(uuid.uuid4()), max_length=100)
    tasks = JSONField(blank=True, default=[])
    traceback = models.TextField(blank=True)
    result = JSONField(blank=True, default=None)
    execution_time = models.FloatField(blank=True, null=True)
    
    def _get_status_cache_key(self):
        """
        Returns the key where job's status would be, if in force.
        """
        template = 'cacheqJob:%d:status'
        return template % self.pk
    
    def _get_cache_client(self):
        """
        Returns the cache client assigned to cacheq in settings.CACHEQ.
        """
        _cqsettings = getattr(settings, 'CACHEQ', {})
        using = _cqsettings.get('CACHE', 'default')
        return get_cache(using)
    
    def save(self, *args, **kwargs):
        """
        Overloads save method in order to set job's status in cache, if in force.
        """
        super(Job, self).save(*args, **kwargs)
        # get cache key and cache client
        key = self._get_status_cache_key()
        cache_client = self._get_cache_client()
        # *only* if job is still pending should we set status in cache
        if self.status == self.PENDING:
            cache_client.set(key, self.status)
            return
        # else we need to clear cache
        cache_client.delete(key)
    
    def ready(self):
        """
        Checks if job is *not PENDING* anymore. Returns False if it is, True if 
        it's not. Note that this method will first check for job status in cache, 
        so it's perfectly safe to do something like this:
        
        while True:
            if job.ready():
                break
        # do something with job.result
        """
        # check status in cache
        key = self._get_status_cache_key()
        if self._get_cache_client().get(key) == self.PENDING:
            return False
        # cached status will only be pending, so now we need to retrieve job 
        # from database in order to update attr values
        updated_job = Job.objects.get(pk=self.pk)
        # updated_job.status should be either DONE or FAILED (both mean it's 
        # ready), but it could be PENDING if user changed status directly, 
        # which could have created an inconsistency between cache and db values, 
        # so we need to update accordingly, if it's the case
        if updated_job.status == self.PENDING:
            # save will update cached status, then return False
            self.save()
            return False
        # else the job is ready and we can fetch result from databse, update job 
        # and then return True
        for attrname in ['status', 'traceback', 'result', 'execution_time']:
            setattr(self, attrname, getattr(updated_job, attrname))
        return True
    
    def add_task(self, func, *args, **kwargs):
        """
        Appends a new task for execution.
        """
        task = {
            'module': func.__module__,
            'name': func.__name__,
            'args': args,
            'kwargs': kwargs
        }
        self.tasks.append(task)
        self.save()
    
    def pop_task(self):
        """
        Calls self.tasks.pop(0) and imports func in order to return exactly what was 
        pushed as task. If no tasks are left we return None.
        """
        try:
            task = self.tasks.pop(0)
        except IndexError:
            # This will happen if no tasks are left, so we return None.
            return None
        # Let's try to return func, args, kwargs
        func = getattr(importlib.import_module(task['module']), task['name'])
        self.save()
        return (func, task['args'], task['kwargs'])


@receiver(post_delete, sender=Job)
def _clear_cached_status(sender, instance, **kwargs):
    # we need to delete cached status so it won't interfere with 
    # other future Jobs that might get the same pk as a deleted one
    instance._get_cache_client().delete(
        instance._get_status_cache_key())
