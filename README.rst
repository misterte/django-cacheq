=============================
django-cacheq
=============================

Background tasks using django's cache framework.


Quickstart
----------

Install django-cacheq::

    pip install django-cacheq

Requirements

- django>=1.5.1
- jsonfield>=1.0.3
- lockfile>=0.10.2

Add it to your installed apps::

    INSTALLED_APPS = (
        ...
        'cacheq',
    )

And that's it with setup. You can add some basic settings too, but they are not really required.::
    
    CACHES = {
        'default: ...,
        'cacheq': ...,
        'other': ...
    }
    
    CACHEQ = {
        'CACHE': 'cacheq',                      # which cache to use, defaults to 'default'
                                                # note that dummybackend is *not* supported
        'LOCKFILE': '/var/tmp/mycacheq.lock',   # lock file to use if cache is filebased, 
                                                # defaults to '/var/tmp/cacheq.lock'
        # these settings are only for testing
        'MEMCACHED_TESTS_USING': 'memcached',   # which cache to use for running memcached 
                                                # backend tests, only for development
        'REDIS_TESTS_USING': 'redis',           # which cache to use for running redis 
                                                # backend tests, only for development
    }

Then use it in your project::

    import operator
    from cacheq import CacheQ
    
    cq = CacheQ(name='myqueue', using='cacheq') # as in get_cache('cacheq')
    
    # either enqueue one job
    job = cq.enqueue(operator.add, 1, 2)
    
    # or several at a time. note that both the args=[...] and kwargs={...}
    # arguments are required in this case, even if empty
    tasks = [(operator.add, [1,2], {}), (operator.div, [], {'a': 2, 'b': 2})]
    job = cq.enqueue_many(tasks) # job with many tasks
    
    # or you can use the @cq.job decorator
    @cq.job
    def myfunc(a,b):
        return a+b
    
    job = myfunc.delay(1,b=2)
    
    # then wait for results
    job.ready() # False
    job.ready() # True
    job.result # 3
    
    # calling job.ready() or job.result will not hit the database
    # it will look for result and status in cache. once it's ready 
    # it will update job in database.

Running the worker::

    python manage.py cqworker --using=cacheq --queue=myqueue --name=worker123 --pulse=0.1

This will run a cqworker with name "worker123" in foreground listening to queue "myqueue" using the cache backend under get_cache('cacheq'). The 'pulse' option is not really necessary, but it will accept any value between 0.0 and 1.0, which will be the time that the worker will wait to look for a new job again. I don't know if this is really helpful, as it would still be only one connection to memcached / redis, and time.sleep is blocking.

These are the default values

- using: 'default'
- queue: 'default'
- name: 'worker'
- pulse: 1.0

When running tests it's helpful to run the worker and exit when jobs are done. You can do this by either calling the cqworker command with the --burst option or by using the worker.run method.::

    python manage.py cqworker --using=cacheq --queue=myqueue --burst
    
    # or programatically
    from cacheq import get_worker
    
    worker = get_worker(queue_name='myqueue', using='cacheq')
    worker.run(burst=True)


django-cacheq uses django ORM as a backend for job results. This is only something that fitted specific needs I had at the time I wrote this package, but I guess it would be wise to remove it at some point and replace it by a cache backend too, or maybe adding a setting that allows other database to be used specifically as a results backend.

Anyways, for now you can clear jobs by using the cqclear command::

    python manage.py cqclear <done failed pending all> [--no-input]
    
In the case you want to delete pending jobs, you will have to confirm the action if you do not provide the --no-input option. So have this in mind if you wish to use a cronjob to clear jobs periodically.

