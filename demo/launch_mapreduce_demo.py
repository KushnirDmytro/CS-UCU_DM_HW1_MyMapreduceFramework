import sys
import os
import time

from subprocess import Popen, PIPE
import json

from MapReduceManeger import MapReduceManeger


help_msg = "Advisable form of launch python3.6 launch_mapreduce_demo.py <config_filename>"

if ('-h' in sys.argv or '--help' in sys.argv):
    print (help_msg)

default_config_file = "config.json"


if len (sys.argv) > 1:
    config_filename = sys.argv[1]
    try:
        with open(config_filename) as cfg:
            config = json.loads(cfg.read())
    except FileExistsError:
        print("config file reading from {} problem".format(config_filename))
else:
    config_filename = default_config_file



cmd = "example_word_counter_mapper.py data.txt worker_{}_"



start = time.perf_counter()



mappers=[]
for i in range(1):
    cmd.format(i)
    mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "worker_{}_".format(i)], stdout=PIPE, bufsize=1))

for pr in mappers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()


mappers_exit_codes = [p.wait() for p in mappers]


reducers = []
for i in range(1):
    cmd.format(i)
    reducers.append(Popen([sys.executable, "-u", "example_word_counter_reducer.py", "res_worker_0_.txt", "reducer_{}_".format(i)],
                          stdout=PIPE, bufsize=1))

for pr in reducers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()


mappers_exit_codes = [p.wait() for p in reducers]




end = time.perf_counter()

print ("launcher", 1000000 * (end - start) , ' msec')