from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from cacheq.models import Job




class Command(BaseCommand):
    help = """Clears existing Jobs from databse."""
    args = "<done failed pending all>"
    
    def handle(self, *args, **options):
        args_lower = [a.lower() for a in args]
        done = 'done' in args_lower
        failed = 'failed' in args_lower
        pending = 'pending' in args_lower
        _all = 'all' in args_lower
        if not True in [done, failed, pending, _all]:
            print("Please specify at least one option: done, failed, pending, all.")
        if _all:
            print("About to remove all Jobs form databse! This includes pending jobs.")
            self.describe_jobs()
            user_input = self.get_input("Proceed?", ['y', 'n'])
            if user_input == 'y':
                print("deleting %d jobs..." % Job.objects.count())
                Job.objects.all().delete()
                print("done.")
        print("Deleting jobs: %s" % ", ".join(args_lower))
        self.describe_jobs()
        q = Q()
        if done:
            q |= Q(status=Job.DONE)
        if failed:
            q |= Q(status=Job.FAILED)
        if pending:
            print("About to remove pending jobs!")
            user_input = self.get_input("Proceed?", ['y', 'n'])
            if user_input == 'n':
                return
            q |= Q(status=Job.PENDING)
        jobs = Job.objects.filter(q)
        print("deleting %d jobs..." % jobs.count())
        jobs.delete()
        print("done.")
    
    def describe_jobs(self):
        print("Job.PENDING: %d" % Job.objects.filter(status=Job.PENDING).count())
        print("Job.FAILED: %d" % Job.objects.filter(status=Job.FAILED).count())
        print("Job.DONE: %d" % Job.objects.filter(status=Job.DONE).count())
        print("All: %d" % Job.objects.count())
    
    def get_input(self, message, options):
        print(message)
        user_input = raw_input(" / ".join(options))
        while True:
            if not user_input in options:
                print("please use a valid option ( %s ):" % " / ".join(options))
            else:
                break
        return user_input
