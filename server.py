#coding=utf-8
#!/usr/bin/env python

from tornado.netutil import TCPServer
from tornado.ioloop  import IOLoop
import struct

from gearman import protocol
from gearman.job import GearmanJob, GearmanJobRequest

from Queue import Queue
from collections import defaultdict

class Dispatcher(object):
    def __init__(self):
        self.register_tasks = defaultdict(list)
        self.request_task = defaultdict(Queue)
    
    def push_request(self, request, task):
        self.request_task[task].put(request)
        
    def unregister_task(self, task, worker):
        self.register_tasks[task].remove(worker)
    
    def register_task(self, task, worker):
        self.register_tasks[task].append(worker)
    
    def dispatch(self, request,task):
        workers  = self.register_tasks[task]
        for worker in workers:
            if worker.status == 0:
                worker.noop(request)
                return
        self.push_request(request,task)

    def dispatch_worker(self, worker):
        for task in worker.tasks:
            self.dispatch_worker_task(worker, task)
    
    def dispatch_worker_task(self, worker, task):
        try:
            request = self.request_task[task].get_nowait()
        except:
            return
        
        if worker.status == 0:
            worker.noop(request)
    
class Connection(object):
    def __init__(self, stream, address, dispatcher):
        #status 0 空闲  1 忙
        self.status = 0
        self.dispatcher = dispatcher
        self.tasks = []
        self.request = None
        self._incoming_buffer = ''
        self._stream = stream
        self._address = address
        self._stream.set_close_callback(self.on_close)        
        self.read_command()
        print address, "A new connection has entered."

    def isbusy(self):
        return status
    
    def read_command(self):
        self._stream.read_bytes(protocol.COMMAND_HEADER_SIZE, self.read_command_arg)

    def read_command_arg(self, buf):
        self._incoming_buffer = buf
        magic, cmd_type, cmd_len = struct.unpack('!4sII', buf)
        self._stream.read_bytes(cmd_len, self.execute_command)
   
    def execute_command(self, buf):
        self._incoming_buffer += buf
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(self._incoming_buffer, False)
        print protocol.get_command_name(cmd_type), cmd_args, cmd_len
       
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
        for task in self.tasks:            
            #register_tasks[task].remove(self)
            self.dispatcher.unregister_task(task, self)
            
        #
        #GEARMAN_COMMAND_WORK_FAIL 14
        if self.request:
            client = self.request.gearman_job.connection
            client.work_fail(self.request.gearman_job.handle)
        print self._address, "A user has left."
    
    
    def grab_job(self):
        self.dispatcher.dispatch_worker(self)

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
        
    def work_fail(self, job_handle):
        """Job Server -> Client"""
        buf = protocol.pack_binary_command(protocol.GEARMAN_COMMAND_WORK_FAIL, {'job_handle':job_handle}, True)
        self._stream.write(buf)



    def recv_reset_abilities(self):
        pass
    
    def recv_can_do(self, task):
        """Worker -> Job Server worker向server注册task"""
        self.tasks.append(task)
        #register_tasks[task].append(self)
        self.dispatcher.register_task(task, self)
        if not self.request:
            self.grab_job()
        

    def recv_grab_job_uniq(self):
        """Worker -> Job Server work向server请求task"""
        if not self.request:
            self.grab_job()
            
        if self.request:
            job = self.request.gearman_job
            self.job_assign_uniq(job.handle, job.task, job.unique, job.data)
            self.status = 1
        else:
            self.no_job()
    
    def recv_pre_sleep(self):
        """Worker -> Job Server worker通知server worker产将进入休眠状态"""
        self.status = 0
       
    def recv_work_complete(self, job_handle, data):
        """Worker -> Job Server worker通知server worker和任务执行完毕, 回传任务结果"""
        #req = self.request
        #req.result = data
        
        client_connection = self.request.gearman_job.connection
        client_connection.work_complete(job_handle, data)
        self.request = None

    def recv_submit_job(self, task, unique, data):
        """Client -> Job Server """
        handle = 'H:lap:1'
        job = GearmanJob(self, handle, task, unique, data)
        request = GearmanJobRequest(job)
        self.job_created(handle)
        
        self.dispatcher.dispatch(request, task)

dispatcher = Dispatcher()    

class GearmanServer(TCPServer):
    def handle_stream(self, stream, address):       
        Connection(stream, address, dispatcher)

if __name__ == '__main__':
    print "begin..."
    server = GearmanServer()
    server.listen(8000)
    IOLoop.instance().start()    