#coding=utf-8
#!/usr/bin/env python

from tornado.netutil import TCPServer
from tornado.ioloop  import IOLoop
import struct

from gearman import protocol
from gearman.job import GearmanJob, GearmanJobRequest

from collections import defaultdict

#worker已注册的任务
#{'echo':[work1, worker2]}
register_tasks = defaultdict(list)


#client请求request dict
#{'H:lap:1':req1}
request_dict = {}

#client请求request list
#{req1, req2, req3}
#request_task = []
request_task = defaultdict(list)

class Connection(object):
    def __init__(self, stream, address):
        #status 0 空闲  1 忙
        self.status = 0
        self.tasks = []
        self.request = None
        self._incoming_buffer = ''
        self._stream = stream
        self._address = address
        self._stream.set_close_callback(self.on_close)        
        self.read_command()
        print address, "A new connection has entered."
   
    def read_command(self):
        self._stream.read_bytes(protocol.COMMAND_HEADER_SIZE, self.read_command_arg)

    def read_command_arg(self, buf):
        self._incoming_buffer = buf
        magic, cmd_type, cmd_len = struct.unpack('!4sII', buf)
        self._stream.read_bytes(cmd_len, self.execute_command)
   
    def execute_command(self, buf):
        self._incoming_buffer += buf
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(self._incoming_buffer, False)
        print cmd_type, cmd_args, cmd_len
       
        gearman_command_name = protocol.get_command_name(cmd_type)
        recv_command_function_name = gearman_command_name.lower().replace('gearman_command_', 'recv_')
        cmd_callback = getattr(self, recv_command_function_name, None)
        if not cmd_callback:
            missing_callback_msg = 'Could not handle command: %r - %r' % (protocol.get_command_name(cmd_type), cmd_args)
            #gearman_logger.error(missing_callback_msg)
            #raise UnknownCommandError(missing_callback_msg)
            raise Exception(missing_callback_msg)

        # Expand the arguments as passed by the protocol
        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(**cmd_args)
       
       
        self.read_command()
   
    def on_close(self):
        print self._address, "A user has left."
    
    def recv_reset_abilities(self):
        print "reset_abilities"
    
    def recv_can_do(self, task):
        """Worker -> Job Server worker向server注册task"""
        print task
        self.tasks.append(task)
        register_tasks[task].append(self)
        if not self.request:
            self.grab_job()
        

    def grab_job(self):
        for task in self.tasks:
            print "aaaa"
            if request_task[task]:
                print len(request_task[task])
                if len(request_task[task]) > 0:
                    request = request_task[task].pop()
                    #request_task[task].remove(request)
                    self.request = request
                    if self.status == 0:
                        self.noop(request)
                    break;

    def recv_grab_job_uniq(self):
        """Worker -> Job Server work向server请求task"""
        print "recv_grab_job_uniq"
        #requests = request_task['reverse']
        #if requests:
        #    req = requests[0]
        #    job = req.gearman_job
        #    #self.job_assign(job.handle, job.task, job.data)
        #    print "job.task:", job.handle, job.task, job.unique, job.data
        #    self.job_assign_uniq(job.handle, job.task, job.unique, job.data)
        #    self.status = 1
        #    requests.remove(req)
        #else:
        #    self.no_job()
        if not self.request:
            self.grab_job()
            
        if self.request:
            job = self.request.gearman_job
            #self.job_assign(job.handle, job.task, job.data)
            print "job.task:", job.handle, job.task, job.unique, job.data
            self.job_assign_uniq(job.handle, job.task, job.unique, job.data)
            self.status = 1
        else:
            self.no_job()
    
    def recv_pre_sleep(self):
        """Worker -> Job Server worker通知server worker产将进入休眠状态"""
        self.status = 0
       
    def recv_work_complete(self, job_handle, data):
        """Worker -> Job Server worker通知server worker和任务执行完毕, 回传任务结果"""
        #req = request_dict[job_handle]
        #req.result = data
        req = self.request
        req.result = data
        
        client_connection = req.gearman_job.connection
        client_connection.work_complete(job_handle, data)

    def recv_submit_job(self, task, unique, data):
        """Client -> Job Server """
        handle = 'H:lap:1'
        job = GearmanJob(self, handle, task, unique, data)
        request = GearmanJobRequest(job)
        #request_dict[handle] = request
        request_task[task].append(request)
        
        self.job_created(handle)
        
        #worker_connection  = register_tasks[task][0]
        #worker_connection.noop()
        
        #
        workers  = register_tasks[task]
        for worker in workers:
            if worker.status == 0:
                worker.noop(request)
                break
    
    def no_job(self):
        """Job Server -> Worker"""
        data = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_NO_JOB, {}, True)
        self._stream.write(data)

    def noop(self, req):
        """Job Server -> Worker"""
        data = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_NOOP, {}, True)
        self._stream.write(data)
        self.request = req

    def job_assign(self, job_handle, task, data):
        """Job Server -> Worker"""
        buf = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_JOB_ASSIGN, {'job_handle':job_handle, 'task':task, 'data':data}, True)
        self._stream.write(buf)

    def job_assign_uniq(self, job_handle, task, unique, data):
        """Job Server -> Worker"""
        buf = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_JOB_ASSIGN_UNIQ, {'job_handle':job_handle, 'task':task, 'unique':unique, 'data':data}, True)
        print "buf:", buf
        self._stream.write(buf)

    def job_created(self, job_handle):
        """Job Server -> Client"""
        data = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_JOB_CREATED, {'job_handle':job_handle}, True)
        self._stream.write(data)
    
    def work_complete(self, job_handle, data):
        """Job Server -> Client"""
        buf = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_WORK_COMPLETE, {'job_handle':job_handle, 'data':data}, True)
        self._stream.write(buf)
        #del request_dict[job_handle]
    

class GearmanServer(TCPServer):
    def handle_stream(self, stream, address):       
        Connection(stream, address)

if __name__ == '__main__':
    print "begin..."
    server = GearmanServer()
    server.listen(8000)
    IOLoop.instance().start()    