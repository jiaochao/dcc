# dcc

```
  usage: 
  exe master -n <num_workers> -s <launch_script> -c <fname>    # run master with worker, fname is file for write server addr
  exe worker -m <master_addr:port> -c <fname> -w <worker_id>   # run worker, fname is for server addr
  exe client -m <master_addr:port> -c <fname> -r task          # run command task can be 'exit' 'cmd gcc ...'
```
