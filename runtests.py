import sys


# caches
CACHES = {
    # default is filebased
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': '/var/tmp/cacheqtest_filebased',
        'TIMEOUT': 300,
        'KEY_PREFIX': 'cacheqtests'
    },
    'memcached': {
        'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
        'LOCATION': 'unix:/var/tmp/cacheqtest_memcached.sock',
        'TIMEOUT': 300,
        'KEY_PREFIX': 'cacheqtests'
    },
    'redis': {
        'BACKEND': 'redis_cache.RedisCache',
        'LOCATION': 'unix:/var/tmp/cacheqtest_redis.sock',
        'TIMEOUT': 300,
        'KEY_PREFIX': 'cacheqtests',
        'OPTIONS': {
            'DB': 0
        },
}
CACHEQ = {
    'CACHE': 'default',                     # defaults to 'default' anyway
    'LOCKFILE': '/var/tmp/cacheqtest.lock', # defaults to 'var/tmp/cacheq.lock'
    'MEMCACHED_TESTS_USING': 'memcached',   # only required if running memcached tests
    'REDIS_TESTS_USING': 'redis',           # only required if running redis tests
}


try:
    from django.conf import settings

    settings.configure(
        DEBUG=True,
        USE_TZ=True,
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
            }
        },
        ROOT_URLCONF="cacheq.urls",
        INSTALLED_APPS=[
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "django.contrib.sites",
            "cacheq",
        ],
        SITE_ID=1,
        NOSE_ARGS=['-s'],
        MIDDLEWARE_CLASSES=(),
        # ADD CACHEQ AND CACHE SETTINGS
        CACHES=CACHES,
        CACHEQ=CACHEQ,
    )

    try:
        import django
        setup = django.setup
    except AttributeError:
        pass
    else:
        setup()

    from django_nose import NoseTestSuiteRunner
except ImportError:
    import traceback
    traceback.print_exc()
    raise ImportError("To fix this error, run: pip install -r requirements-test.txt")


def run_tests(*test_args):
    if not test_args:
        test_args = ['tests']

    # Run tests
    test_runner = NoseTestSuiteRunner(verbosity=1)

    failures = test_runner.run_tests(test_args)

    if failures:
        sys.exit(failures)


if __name__ == '__main__':
    run_tests(*sys.argv[1:])
