import sys
import subprocess
import os
#debug: os.environ['PYTHONASYNCIODEBUG'] = '1'
import asyncio
import logging as log
import struct
import pickle
import typing
import shlex
import shutil
import subprocess
import queue
import time
from datetime import datetime
import pathlib

# master/worker/client 
# master -- coordinate work
# worker -- do the work
# client -- assign single task, including exit
#  usage: 
#  exe master -n <num_workers> -s <launch_script> -c <fname>   # run master with num worker and put address in fname
#  exe worker -m <master_addr:port> -w <worker_id> -c <fname>  # run worker
#  exe client -m <master_addr:port> -c <fname> -r <task>        # run command <task> can be "exit", "cmd ..."
 
def debug_print(msg):
  ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
  log.info(f'{ts}: {msg}')

g_timeout = 1

class Msg:
  def __init__(self):
    self.data = bytearray()
  
  def add_data(self, data):
    self.data.extend(data)

  def get_msgs(self):
    num_bytes_of_size = 8 # size of data size
    msgs = []
    while len(self.data) >= num_bytes_of_size:
      num_bytes_of_msg, = struct.unpack_from('!Q',self.data)
      if (num_bytes_of_msg + num_bytes_of_size) <= len(self.data) :
        md = self.data[num_bytes_of_size:num_bytes_of_msg+num_bytes_of_size]
        self.data = self.data[num_bytes_of_msg+num_bytes_of_size:]
        msgs.append(pickle.loads(md))
      else:
        break
    return msgs

  def msg_to_data(self, msg):
    # msg should be tuple (name,data)
    data = pickle.dumps(msg)
    size_data = struct.pack('!Q',len(data))
    return size_data + data
    
class Master():
  def __init__(self, num_workers, launch_script, master_addr_file):
    self.exit_signal = None
    self.server_addr = None
    self.server_port = None
    self.num_workers = num_workers
    self.launch_script = launch_script
    self.protocols= {}
    self.idle_workers = queue.SimpleQueue()
    self.tasks = queue.SimpleQueue()
    self.heartbeat = self.MasterHeartbeat(self)
    self.num_client = 0
    self.master_addr_file = master_addr_file
    self.client_protocols = {}

  def sched(self):
    worker_id = self.get_idle_worker()
    if worker_id is None:
      return
    else:
      task = self.get_task()
      if task is None:
        self.add_idle_worker(worker_id)
      else:
        self.protocols[worker_id].send(('worker_task',task))

  def add_idle_worker(self, worker_id):
    try:
      self.idle_workers.put(worker_id)
    except Exception as ex:
      debug_print(f'Error: add idle worker {worker_id} to queue failed with {ex}. Exit')
      exit(1)

  def get_idle_worker(self):
    worker_id = None
    try:
      worker_id = self.idle_workers.get_nowait()
    except queue.Empty as ex:
      worker_id = None
    except Exception as ex:
      debug_print(f'Error: get idle worker failed with {ex}, Exit')
      exit(1)
    return worker_id

  def add_task(self,task):
    try:
      self.tasks.put(task)
    except Exception as ex:
      debug_print(f'Error: Put task in queue failed with exception {ex}. Exit')
      exit(1)

  def get_task(self):
    task = None
    try:
      task = self.tasks.get_nowait() 
    except queue.Empty as ex:
      task = None
    except Exception as ex:
      debug_print('Error: unknow exception for queue: {ex}. Exit.')
      exit(1)
    return task

  def to_exit(self, exit_msg):
    if not self.exit_signal.done():
      self.exit_signal.set_result(exit_msg)

  def close_clients(self):
    for client_idx in self.client_protocols:
      if self.client_protocols[client_idx] is not None:
        self.client_protocols[client_idx].transport.close()

  class MasterHeartbeat:
    def __init__(self,master):
      self.master = master
      self.timeout = g_timeout # sec
      self.running = None
      
    async def check(self):
      while self.running:
        await asyncio.sleep(self.timeout)
        for worker_id,protocol in master.protocols.items():
          dt = time.time() - protocol.last_update
          if dt > self.timeout * 3:
            debug_print(f'Error: worker timeout {protocol.worker_id}, {dt}. Exit')
            self.master.to_exit({'exit_code':1,'exit_reason':'worker timeout'})
            self.stop()

    def stop(self):
      self.running = False

    def start(self):
      self.running = True
      self.master.check_task = asyncio.create_task(self.check())

    def update(self, worker_id):
      self.master.protocols[worker_id].last_update = time.time()

  class MasterProtocol(asyncio.Protocol):
    def __init__(self, master):
      self.master = master
      self.data = Msg()
      self.last_update = time.time()
      self.transport = None
      self.worker_id = None
      self.client_idx = None

    def eof_received(self):
      if self.worker_id:
        debug_print(f'eof received from worker {self.worker_id}')
        self.master.to_exit({'exit_code':1,'exit_reason':'eof received from worker'})
        self.transport.close()
      if self.client_idx:
        debug_print(f'eof received from client {self.client_idx}')
        self.master.client_protocols[self.client_idx] = None
    
    def connection_lost(self, exc):
      if self.worker_id:
        debug_print(f'connection lost to worker {self.worker_id}')
        self.master.to_exit({'exit_code':1,'exit_reason':'connection lost to worker'})
        self.transport.close()
      if self.client_idx:
        debug_print(f'connection lost to client {self.client_idx}')
        self.master.client_protocols[self.client_idx] = None
        
    def connection_made(self, transport):
      self.transport = transport
      peer = transport.get_extra_info('peername')
      sock = transport.get_extra_info('sockname')
      debug_print(f'Master connected to {peer} at {sock}')


    def send(self,msg):
      #debug_print(f'send msg {msg} from master to {self.worker_id if self.worker_id else self.client_idx}')
      self.transport.write(self.data.msg_to_data(msg))
 
    def data_received(self, data):
      self.data.add_data(data)
      msgs = self.data.get_msgs()
      for msg_name,msg_data in msgs:
        #debug_print(f'received msg: ({msg_name} {msg_data})')
        if msg_name == 'worker_id':
          self.worker_id = msg_data
          if self.worker_id in self.master.protocols:
            debug_print(f'Error: worker with ID { self.worker_id } created twice!. needs fix')
          self.master.protocols[self.worker_id] = self
          self.master.heartbeat.update(self.worker_id)
        elif msg_name == 'worker_ready':
          self.master.add_idle_worker(self.worker_id)
          self.master.sched()
        elif msg_name == 'worker_task_done':
          task = msg_data
          self.master.client_protocols[task['client_idx']].send(('client_task_done',task))
        elif msg_name == 'heartbeat':
          self.master.heartbeat.update(self.worker_id)
          self.send(('heartbeat',self.worker_id))
        elif msg_name == 'request_server_exit':
          debug_print('requested exit. Exit')
          self.send(('server_exit',None))
          self.master.to_exit({'exit_code':1, 'exit_reason':'requested exit'})
        elif msg_name == 'client_exit':
          client_idx = msg_data
          self.master.client_protocols[client_idx] = None
          self.transport.close()
        elif msg_name == 'client_task':
          cmd = msg_data
          self.master.add_task({'client_idx':self.client_idx, 'cmd':cmd})
          self.master.sched()
        elif msg_name == 'client_ready':
          self.client_idx = self.master.num_client
          self.master.num_client += 1
          self.master.client_protocols[self.client_idx] = self
          self.send(('client_idx',self.client_idx))
        else:
          debug_print('Error: unknow msg {msg_name} {msg_data}')
    
  async def server_main(self):
    debug_print('server main running: ')
    loop = asyncio.get_running_loop()
    self.heartbeat.start() # to start heartbeat
    self.exit_signal = loop.create_future()
    try:
      server = await loop.create_server(lambda: self.MasterProtocol(self),host='localhost',port=None)
    except Exception as ex:
      debug_print(f'Error: exception happen when create server {ex}, Exit')
      exit(1)
    except SystemExit as ex:
      debug_print(f'Error: exception happen {ex}')
      exit(1)
    except:
      debug_print('Error: unknown exception happen...')
      exit(1)

    if not server:
      debug_print('Error: create server failed. Exit!')
      exit(1)

    addr = None
    port = None
    for sock in server.sockets:
      sockname = sock.getsockname()
      if len(sockname) == 2: # ipv4
        addr = sockname[0]
        port = sockname[1]

    if addr == None or port == None:
      debug_print(f'Error: server socket addr and port not get. Exit.')
      exit(1)

    self.server_addr = addr
    self.server_port = port
    debug_print(f'server listening on {addr}:{port}')
    print(f'server started on {addr}:{port}')
    with open(self.master_addr_file,'w') as f:
      f.write(f'{addr}:{port}')
    self.launch_workers(self.num_workers, self.launch_script, self.server_addr, self.server_port)

    try:
      async with server:
        await self.exit_signal
    except Exception as ex:
      debug_print(f'Exception happend, Exit. {ex}')
    finally:
      self.heartbeat.stop()
      self.close_clients() # clients has no heart beat, needs close
      debug_print("Finally Done")
      #self.stat_report()
    debug_print('server main exit.')

  def run(self):
    debug_print('launch master')
    asyncio.run(self.server_main())
      
  def launch_workers(self, num, launch_script, server_addr, server_port):
    exe = sys.executable
    py  = os.path.abspath(sys.argv[0])
    for i in range(0,num):
      cmd = f'{launch_script} {exe} {py} worker -m {server_addr}:{server_port} -w {i}&'
      os.system(cmd)

class Worker:
  class WorkerHeartbeat:
    def __init__(self, worker):
      self.worker = worker
      self.timeout = g_timeout  # sec
      self.running = None
    async def check(self):
      while self.running:
        if self.worker.protocol:
          self.worker.protocol.send(('heartbeat',self.worker.worker_id))
        await asyncio.sleep(self.timeout)
        dt = time.time() - self.worker.protocol.last_update
        if dt > self.timeout * 3:
          debug_print(f'Error: heart beat time out for worker id: {self.worker.worker_id} {dt}. Exit.')
          self.worker.to_exit({'exit_code':1, 'exit_reason':'heartbeat timeout'})
          self.stop()
    
    def stop(self):
      self.running = False 
    def start(self):
      self.running = True
      self.worker.check_task = asyncio.create_task(self.check())

    def update(self):
      self.worker.protocol.last_update = time.time()

  class WorkerProtocol(asyncio.Protocol):
    def __init__(self, worker):
      self.worker = worker
      self.data = Msg()
      self.transport = None
      self.last_update = time.time()

    def send(self, msg):
      #debug_print(f'send msg {msg} from worker {self.worker.worker_id} to master')
      self.transport.write(self.data.msg_to_data(msg))

    def eof_received(self):
      debug_print(f'eof received from master on worker {self.worker.worker_id}')
      self.worker.to_exit({'exit_code':1,'exit_reason':'eof received from master'})
      self.transport.close()

    def connection_lost(self, exc):
      debug_print(f'connection lost to master on worker {self.worker.worker_id}')
      self.worker.to_exit({'exit_code':1,'exit_reason':'connection lost to master'})
      self.transport.close()
       
    def connection_made(self, transport):
      peer = transport.get_extra_info('peername')
      sock = transport.get_extra_info('sockname')
      debug_print(f'connected to {peer} at {sock}')
      self.transport = transport
      self.worker.protocol = self
      self.send(('worker_id',self.worker.worker_id))
      self.send(('worker_ready',self.worker.worker_id))

    def data_received(self, data):
      self.data.add_data(data)
      msgs = self.data.get_msgs()
      for msg_name, msg_data in msgs:
        #debug_print(f'received msg: ({msg_name} {msg_data})')
        if msg_name == 'worker_task':
          worker_task = msg_data
          asyncio_task = asyncio.create_task(self.run_cmd(worker_task))
          asyncio_task.add_done_callback(self.run_cmd_done)
        elif msg_name == 'heartbeat':
          self.worker.heartbeat.update()

    def run_cmd_done(self, asyncio_task):
      if asyncio_task.cancelled():
        debug_print(f'Error: task cancelded.')
        return
      worker_task = asyncio_task.result()
      self.send(('worker_task_done',worker_task))
      self.send(('worker_ready',self.worker.worker_id))

    def run_shell_cmd(self, worker_task):
      worker_task['exit_code'] = os.system(worker_task['cmd'])

    async def run_cmd(self, worker_task):
      loop = asyncio.get_running_loop()
      result = await loop.run_in_executor( None, lambda: self.run_shell_cmd(worker_task))
      return worker_task
    
  def __init__(self, server_addr, server_port, worker_id):
    self.worker_id = worker_id
    self.server_addr = server_addr
    self.server_port = server_port
    self.protocol = None
    self.heartbeat = self.WorkerHeartbeat(self)
    self.exit_signal = None

  def to_exit(self,exit_msg):
    if not self.exit_signal.done():
      self.exit_signal.set_result(exit_msg)

  async def worker_main(self):
    loop = asyncio.get_running_loop()
    self.heartbeat.start()
    self.exit_signal = loop.create_future()
    debug_print(f'connect to {self.server_addr}:{self.server_port}')
    try:
      transport, protocol = await loop.create_connection(lambda: self.WorkerProtocol(self), self.server_addr, self.server_port) 
    except Exception as ex:
      debug_print(f'Error: exception happen when create connection to {self.server_addr} {self.server_port} {ex}, Exit')
      exit(1)
    except:
      debug_print(f'Exception happend. Exit')
      exit(1)

    try:  
      await self.exit_signal
    finally:
      transport.close()
      self.heartbeat.stop()
      debug_print(f'Worker {self.worker_id} Exit. ')

  def run(self):
    asyncio.run(self.worker_main())

class Client:
  class ClientProtocol(asyncio.Protocol):
    def __init__(self, client):
      self.client = client
      self.transport = None
      self.data = Msg()

    def send(self, msg):
      #debug_print(f'send msg {msg} from client')
      self.transport.write(self.data.msg_to_data(msg))

    def eof_received(self):
      debug_print(f'eof received from master on client {self.client.client_idx}')
      self.client.to_exit({'exit_code':1,'exit_reason':'eof received from worker'})
      self.transport.close()

    def connection_lost(self, exc):
      debug_print(f'connection lost to master on client {self.client.client_idx}')
      self.client.to_exit({'exit_code':1,'exit_reason':'connection lost to worker'})
      self.transport.close()


    def connection_made(self, transport):
      peer = transport.get_extra_info('peername')
      sock = transport.get_extra_info('sockname')
      debug_print(f'client connected to {peer} at {sock}')
      self.transport = transport
      self.send(('client_ready',None))

    def data_received(self, data):
      self.data.add_data(data)
      msgs = self.data.get_msgs()
      for msg_name, msg_data in msgs:
        #debug_print(f'received msg: ({msg_name} {msg_data})')
        if msg_name == 'client_idx':
          self.client.client_idx = msg_data
          if 'exit' in self.client.task:
            self.send(('request_server_exit',self.client.client_idx))
          elif 'cmd' in self.client.task:
            self.send(('client_task',self.client.task['cmd']))
          else:
            debug_print(f'Error: wrong task get {self.client.task}. Exit')
            self.client.to_exit({'exit_code':1, 'exit_reason':'task not supported'})
        elif msg_name == 'server_exit':
          self.client.to_exit({'exit_code':0,'exit_reason':'request server exit done'})
        elif msg_name == 'client_task_done':
          task = msg_data
          self.send(('client_exit',self.client.client_idx))
          self.client.to_exit({'exit_code':task['exit_code'], 'exit_reason': task})

  def __init__(self, server_addr, server_port, task):
    self.client_idx = None
    self.server_addr = server_addr
    self.server_port = server_port
    self.task = task
    self.exit_signal = None

  def to_exit(self, exit_msg):
    if not self.exit_signal.done():
      self.exit_signal.set_result(exit_msg)

  async def client_main(self):
    loop = asyncio.get_running_loop()
    self.exit_signal = loop.create_future()
    try:
      transport, protocol = await loop.create_connection(lambda: self.ClientProtocol(self), self.server_addr, self.server_port) 
    except Exception as ex:
      debug_print(f'Error: exception happen when client create connection to {self.server_addr} {self.server_port} {ex}, Exit')
      exit(1)
    except:
      debug_print(f'Error: unknown exception happend. Exit.')
      exit(1)

    try:  
      await self.exit_signal
      task = self.exit_signal.result()
      exit(task['exit_code'])
    finally:
      transport.close()
      debug_print(f'Client {self.client_idx} Exit. ')
      exit(1)

  def run(self):
    asyncio.run(self.client_main())
    


def print_usage(msg):
  usage = '''
  usage: 
  exe master -n <num_workers> -s <launch_script> -c <fname>    # run master with worker, fname used to write server addr
  exe worker -m <master_addr:port> -c <fname> -w <worker_id>   # run worker ( only run in master )
  exe client -m <master_addr:port> -c <fname> -r task          # run command, task can be 'exit', 'cmd gcc ...'
  '''
  print(usage)
  print(msg)
  debug_print(usage)
  debug_print(msg)

def parse_args():
  ret = {}
  if len(sys.argv) < 2:
    print_usage('Error: only two arguments, needs more. Exit')
    exit(1)
  if sys.argv[1] in ['master','worker','client']:
    ret['mode'] = sys.argv[1];
  else:
    print_usage(f'Error: {sys.argv[1]} is not supported. Exit')
    exit(1)

  i = 2
  while i < len(sys.argv):
    if sys.argv[i] == '-n':
      i += 1
      if i < len(sys.argv):
        ret['num_workers'] = int(sys.argv[i])
      else:
        print_usage('Error: -n needs to specify number of workers. Exit')
        exit(1)
    elif sys.argv[i] == '-s':
      i += 1
      if i < len(sys.argv):
        ret['launch_script'] = sys.argv[i]
      else:
        print_usage('Error: -s needs to specify scripts. Exit')
        exit(1)
    elif sys.argv[i] == '-m':
      i += 1
      if i < len(sys.argv):
        addr_port = sys.argv[i].split(':')
        if len(addr_port) != 2:
          print_usage(f'Error: worker -m option not recognized, needs format addr:port got {sys.argv[i]}. Exit')
          exit(1)
        ret['master_addr'] = addr_port[0]
        ret['master_port'] = addr_port[1]
      else:
        print_usage('Error: -m needs to specify master addr. Exit')
        exit(1)
    elif sys.argv[i] == '-w':
      i += 1
      if i < len(sys.argv):
        ret['worker_id'] = sys.argv[i]
      else:
        print_usage('Error: -w needs to specify worker id. Exit')
        exit(1)
    elif sys.argv[i] == '-c':
      i += 1
      if i < len(sys.argv):
        ret['master_addr_file'] = sys.argv[i]
        if ret['mode'] != 'master':
          if not os.path.exists(ret['master_addr_file']):
            print_usage(f'Error: file {ret["master_addr_file"]} does not exists. Exit')
            exit(1)
          with open(ret['master_addr_file']) as f:
            addr_port = f.read()
          addr_port = addr_port.strip()
          addr_port = addr_port.split(':')
          if len(addr_port) != 2:
            print_usage(f'Error: file {ret["master_addr_file"]} format wrong, needs addr:port, Exit')
            exit(1)
          ret['master_addr'] = addr_port[0]
          ret['master_port'] = addr_port[1]
      else:
        print_usage('Error: -c needs to specify file name with master addr')
    elif sys.argv[i] == '-r':
      i += 1
      if i < len(sys.argv):
        ret['task'] = {sys.argv[i]:' '.join([shlex.quote(sys.argv[i]) for i in range(i+1,len(sys.argv))])}
        i = len(sys.argv)
      else:
        print_usage('Error: -r needs to specify arguments. Exit')
        exit(1)
    else:
      print_usage(f'Error: master mode does not support option {sys.argv[i]}, Exit!')
      exit(1)
    # next
    i += 1
  return ret

if __name__ == '__main__':
  log_dir = pathlib.Path('log.dcc')
  if not log_dir.exists():
    log_dir.mkdir()

  log.basicConfig(filename=log_dir/f'my.log.{os.getpid()}', filemode='w', level=log.INFO)
  #log.basicConfig(filename=f'my.log.{os.getpid()}', filemode='w', level=log.DEBUG)
  #log.basicConfig(level=log.DEBUG)
  debug_print(f'CWD: {os.getcwd()}')
  debug_print(f'CMD: {" ".join(sys.argv)}')
  arg = parse_args()

  if arg['mode'] == 'master':
    num_workers = arg['num_workers'] if 'num_workers' in arg else 1
    launch_script = arg['launch_script'] if 'launch_script' in arg else ''
    master_addr_file = arg['master_addr_file'] if 'master_addr_file' in arg else None
    if master_addr_file is None:
      print_usage('Error: master needs to specify -c. Exit')
      exit(1) 
    master = Master(num_workers, launch_script, master_addr_file)
    master.run()
    
  elif arg['mode'] == 'worker':
    if 'master_addr' not in arg or 'master_port' not in arg or 'worker_id' not in arg:
      print_usage('Error: worker needs to specify -m or -c and -w. Exit')
      exit(1)
    worker = Worker(arg['master_addr'], arg['master_port'], arg['worker_id'])
    worker.run()

  elif arg['mode'] == 'client':
    if 'master_addr' not in arg or 'master_port' not in arg:
      print_usage('Error: client mode needs to specify addr and port. Exit')
      exit(1) 
    if 'task' not in arg:
      print_usage('Error: client mode needs to specify tasks with -c. Exit')

    client = Client(arg['master_addr'], arg['master_port'], arg['task'])
    client.run()
  else:
    print_usage()
    debug_print(f'Error: internal error, not supported mode {arg["mode"]}, Exit')
    exit(1)
