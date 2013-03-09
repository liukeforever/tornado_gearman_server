from gearman import *
import time

def echotime(gearman_worker, job):
    return time.ctime()

def reverse(gearman_worker, job):
    return job.data[::-1]    

worker = GearmanWorker(['localhost:8000'])
worker.register_task("reverse", reverse)
worker.work()
