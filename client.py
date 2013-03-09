from gearman import *
import time

client = GearmanClient(['localhost:8000'])
current_request = client.submit_job('reverse', data='world', background=False, wait_until_complete=True)
result = current_request.result
print result