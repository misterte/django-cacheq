from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from cacheq import get_worker




class Command(BaseCommand):
    help = """Runs a worker named 'worker' polling a specific queue named 
    'queue', in get_cache('using') and an idle wait time of 'pulse' seconds. 
    If the 'burst' option is used, the worker will run all pending jobs and exit."""
    
    option_list = BaseCommand.option_list + (
        make_option('-u', '--using', action='store', type='string', dest='using',
            default='default', help="Cache to use, as in get_cache('mycache')."),
        make_option('-q', '--queue', action='store', type='string', dest='queue_name',
            default='default', help="Queue name, defaults to 'default'."),
        make_option('-n', '--name', action='store', type='string', dest='worker_name',
            default='worker', help="Worker name, defaults to 'worker'."),
        make_option('-p', '--pulse', action='store', type='float', dest='pulse',
            default=1.0, help="Time to wait between every time a worker looks for a new job in queue."),
        make_option('-b', '--burst', action='store_true', dest='burst', 
            default=False, help="Run worker in burst mode, which will run pending jobs and exit."))
    
    def handle(self, *args, **options):
        worker = get_worker(queue_name=options['queue_name'], 
                            using=options['using'],
                            worker_name=options['worker_name'],
                            pulse=options['pulse'])
        worker.run(burst=options['burst'])