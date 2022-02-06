import sys
import subprocess
import os
import asyncio
import logging as log

# master/worker/client 
# master -- coordinate work
# worker -- do the work
# client -- assign single task, including exit
# usage:
#   dcc -n 10 -s '' -f <fname>  # launch server w/ 10 worker, put addr in fname
#   dcc -m <addr> -w <worker id># launch worker 
#   dcc -r <addr> --r <gcc ... ># launch gcc command on cluster
#   dcc -r <addr> --e           # exit cluster

# heart beat support
# using asyncio

class md:
  exit_signal = None
  server = None
  master_addr = None
  num_worker = None
  launch_script = None


class Master(asyncio.Protocal):
  def __init__(self):
    pass
  
  def connection_made(self, transport):
    pass

class Worker(asyncio.Protocal):
  pass

class Client(asyncio.Protocal):
  pass

async def server_main():
  loop = asyncio.get_running_loop()
  md.exit_signal = loop.create_future()
  try:
    server = await loop.create_server(lambda: Master())
  except Exception as ex:
      log.info(f'Error: exception happedn when create server {ex}, Exit')
      exit(1)

  if not server:
    log.info('Error: create server failed. Exit!')
    exit(1)

  addr,port = server.sockets[0].getsockname()
  md.master_addr = f'{addr}:{port}'
  log.info(f'server listening on {addr}:{port}')
  launch_workers(md.num_worker, md.launch_script, md.master_addr)

  try:
    async with server:
      await md.exit_signal
  finally:
    loop.stop()
    md.stat_report()


def launch_master():
  log.info('launch master')
  asyncio.run(server_main())
    
def launch_workers(num, launch_script, master_addr):
  exe = sys.executable
  py  = os.path.abspath(sys.argv[0])
  for i in range(0,num):
    cmd = f'{launch_script} {exe} {py} -m {master_addr} -w w{i}&'
    os.system(cmd)

def launch_worker(master_addr, worker_id):
  print(f'worker {worker_id} launched')

def print_usage():
  usage = '''
  usage: exe 
  -n <num_workers> -s <launch_script> 
  -m <master_addr:port> -w <worker_id>
  '''
  print(usage)

def parse_args():
  ret = {}
  n = len(sys.argv)-1
  if n%2 != 0:
    print('parameter count incorrect.')
    print_usage()
    exit(1)
  for i in range (0,n/2):
    k = sys.argv[2*i+1]
    v = sys.argv[2*i+2]
    if k == '-n':
      ret['num_workers'] = int(v)
    elif k == '-s':
      ret['launch_script'] = v
    elif k == '-m':
      ret['master_addr'] = v
    elif k == '-w':
      ret['worker_id' = v
    else:
      print('unknown paramter')
      print(f'{k}')
      print('exit!')
      exit(1)
  return ret

if __name__ == '__main__':

  arg = parse_args(sys.argv)

  num_workers = arg['num_workers'] if 'num_workers' in arg else None
  launch_script = arg['launch_script'] if 'launch_script' in arg else ''
  master_addr = arg['master_addr'] if 'master_addr' in arg else None
  worker_id = arg['worker_id'] if 'worker_id' in arg else None

  if master_addr:
    start_worker(master_addr, worker_id)

  if num_workers:
    master_addr = launch_master()
    launch_workers(num_workers, launch_script, master_addr) 

