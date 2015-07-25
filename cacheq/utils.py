# -*- coding: utf-8 -*-
import os
from collections import deque
import uuid
from lockfile import LockFile, AlreadyLocked, NotLocked, NotMyLock
from django.core.cache import get_cache
from django.core.cache.backends.memcached import MemcachedCache
from django.core.cache.backends.dummy import DummyCache
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings




class CacheKeyRegistry(object):
    """
    Manages a *unique* cache key registry in cache. Note that this *is not* thread-safe, 
    so it should run within a thread-safe environment to be considered so.
    """
    def __init__(self, registry_key, client, timeout=None):
        self.registry_key = registry_key
        self.client = client
        self.timeout = timeout or self.client.default_timeout
    
    def all(self):
        """
        Returns current keys in registry.
        """
        # keys could have timed out
        return self.client.get(self.registry_key, deque([]))
    
    def add(self, key):
        """
        Adds new key to registry.
        """
        keys = self.all()
        keys.append(key)
        self.client.set(self.registry_key, keys, self.timeout)
    
    def pop(self):
        """
        Pops first key from registry.
        """
        keys = self.all()
        try:
            key = keys.popleft()
            self.client.set(self.registry_key, keys, self.timeout)
            return key
        except IndexError:
            # no keys in registry
            return None
    
    def remove(self, key):
        """
        Removes key from registry.
        """
        keys = self.all()
        try:
            # attempt to remove key. note that this 
            # method assumes all keys are unique.
            keys.remove(key)
            self.client.set(self.registry_key, keys, self.timeout)
        except ValueError:
            # key was not in registry
            pass
    
    def reset(self):
        """
        Resets all keys from registry. Be careful when using this, as 
        you could affect other programs using the registry.
        """
        self.client.set(self.registry_key, deque([]), self.timeout)


class BaseCacheQueue(object):
    """
    This object creates a non-blocking thread-safe FIFO queue using django's cache api. 
    Note that only some backends support atomic operations, so the .lock() and .release() 
    methods should be defined in subclasses, depending on the cache backend in use.
    """
    def __init__(self, name, client=None):
        """
        General setup.
        """
        # setup
        _cqsettings = getattr(settings, 'CACHEQ', {})
        self.lockfile = _cqsettings.get('LOCKFILE', '/var/tmp/cacheq.lock')
        self.client = client or get_cache(_cqsettings.get('CACHE', 'default'))
        self.name = name
        # lock and key registry need to last  indefinately
        # so we set timeout accordingly
        self.lock_key = 'CacheQueue:%s:lock' % self.name
        self.timeout = max(self.client.default_timeout, 60*60*24*365*2)
        # check compatibility & ping cache
        self.check_compatibility()
        self.ping_cache()
        # add a key registry. this registry is what really manages the queue.
        # it is not thread-safe, so we need to run it from within CacheQueue.
        self.registry = CacheKeyRegistry(
                registry_key='CacheQueue:%s:registry' % self.name,
                client=self.client, timeout=self.timeout)
    
    # Overload these methods
    ########################
    def check_compatibility(self):
        """
        Checks compatibility with provided client.
        """
        raise NotImplementedError("Please overload this method.")
    
    def lock(self):
        """
        Acquire a lock for thread safe operation.
        """
        raise NotImplementedError("Please overload this method.")
    
    def release(self):
        """
        Release acquired lock.
        """
        raise NotImplementedError("Please overload this method.")
    
    # Base methods
    ###############
    def ping_cache(self):
        """
        Pings cache to check if it's running. Returns True if it is, False if not.
        """
        key = getattr(self, 'ping_key', None)
        if key is None:
            self.ping_key = 'CacheQueue:%s:ping' % self.name
        self.client.set(self.ping_key, True)
        if not self.client.get(self.ping_key, False):
            raise ImproperlyConfigured("Cache not responding, please verify it's running.")
    
    def random_message_key(self):
        """
        Builds a random message key using uuid.
        """
        return 'CacheQueue:%s:message:%s' % (self.name, str(uuid.uuid4()))
    
    def add_message(self, message):
        """
        Attempts a lock. If successful, sets a message and releases.
        """
        if not self.lock():
            return
        # generate a unique random key
        message_key = self.random_message_key()
        # try to set message
        try:
            self.client.set(message_key, message, timeout=self.timeout)
            self.registry.add(message_key)
        except:
            # TODO: log
            # if there was an error, we must cleanup
            self.client.delete(message_key)
            # note that registry.remove assumes all keys are unique
            self.registry.remove(message_key)
        finally:
            # relase lock
            self.release()
    
    def pop_message(self):
        """
        Attempts a lock. If successful, attempts to pop message by FIFO and releases.
        """
        if not self.lock():
            # If there was a lock, we return False, which should be 
            # interpreted as lock being on, not queue being empty
            return False
        # Get message key
        message_key = self.registry.pop()
        # if no message_key was returned, we return none. this should 
        # be interpreted as an empty queue.
        if not message_key:
            # release lock
            self.release()
            return None
        try:
            # else we fetch message
            message = self.client.get(message_key)
            # remove message
            self.client.delete(message_key)
        finally:
            # release lock
            self.release()
        return message
    
    def reset(self):
        """
        Resets queue, release lock and deletes all messages. Be careful with this, 
        as you could affect other programs using the queue.
        """
        self.client.delete_many(self.registry.all())
        self.registry.reset()
        self.release()


class MemcachedCacheQueue(BaseCacheQueue):
    """
    Implements a thread safe cache queue using django's MemcachedCache cache backend. 
    This queue is compatible only with memcached backend, and works completely in memory.
    """
    def check_compatibility(self):
        """
        Checks that backend is MemcachedCache.
        """
        if type(self.client) is not MemcachedCache:
            raise ImproperlyConfigured("This queue only supports MemcachedCache backend or a memcache client.")
    
    def lock(self):
        """
        Method used by MemcachedCache to acquire a lock. Returns True if lock 
        is acquired, else False.
        """
        locked = self.client.add(self.lock_key, 1, timeout=self.timeout)
        # if cache is down, we will get 0
        if locked == 0:
            self.ping_cache()
        # if we acquired a lock, we'll get True
        # if we could not acquire lock, but cache 
        # is running, we'll get False
        return locked
    
    def release(self):
        """
        Method used by MemcachedCache to release a lock.
        """
        # This will always return None, even if cache is down
        self.client.delete(self.lock_key)


class RedisCacheQueue(MemcachedCacheQueue):
    """
    Works exactly the same way as memcached, for now.
    """
    def check_compatibility(self):
        """
        Checks that backend is MemcachedCache.
        """
        if type(self.client).__name__ != 'RedisCache':
            raise ImproperlyConfigured("This queue only supports RedisCache backends.")


class LockFileCacheQueue(BaseCacheQueue):
    """
    Implements a thread safe cache queue using python's lockfile module. This queue is 
    compatible with all cache backends, except DummyCache.
    """
    def check_compatibility(self):
        """
        Checks that backend is supported.
        
        DummyCache:                 not compatible
        Memcached:                  not compatible
        Other backends:             compatible
        Valid self.lockfile:        required
        """
        if type(self.client) in [MemcachedCache, DummyCache]:
            raise ImproperlyConfigured("This queue does not support MemcachedCache nor DummyCache.")
        if not os.path.exists(self.lockfile):
            msg = "%s backend requires the use of a lockfile to work with this queue."
            raise ImproperlyConfigured(msg % type(self.client))
    
    def lock(self):
        """
        Method used for acquiring a lock using the lockfile module.
        """
        lock = LockFile(self.lockfile)
        # check if it's locked
        if lock.is_locked():
            # Note that lock.i_am_locking() could be True, so
            # this apporach is not really efficient from a threading 
            # point of view. However, we must be consistent with 
            # MemcachedCacheQueue's behavior.
            return False
        # else we can attempt to acquire a lock
        # we don't want this to fail silently
        # so we set timeout=0
        lock.acquire(timeout=0)
        return True
    
    def release(self):
        """
        Method used to release a lock using the lockfile module.
        """
        lock = LockFile(self.lockfile)
        if lock.i_am_locking():
            lock.release()


def get_cache_queue(name, using=''):
    """
    Returns an instance of the best alternative of cache queue, given the cache 
    cache to use.
    """
    if not using:
        _cqsettings = getattr(settings, 'CACHEQ', {})
        using = _cqsettings.get('CACHE', 'default')
    cache = get_cache(using)
    if type(cache) is MemcachedCache:
        return MemcachedCacheQueue(name=name, client=cache)
    if type(cache).__name__ == 'RedisCache':
        return RedisCacheQueue(name=name, client=cache)
    # else default to lock file based queue
    return LockFileCacheQueue(name=name, client=cache)

