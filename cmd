#!/bin/csh
rm log.dcc/*
rm master.addr

python3 dcc.py master -n 2 -c master.addr &

sleep 1

python3 dcc.py client -c master.addr  -r cmd ls &
python3 dcc.py client -c master.addr  -r cmd ls -l &
python3 dcc.py client -c master.addr  -r cmd ls -la &
python3 dcc.py client -c master.addr  -r cmd ls -l &
python3 dcc.py client -c master.addr  -r cmd ls -lac &

sleep 20

python3 dcc.py client -c master.addr -r exit
