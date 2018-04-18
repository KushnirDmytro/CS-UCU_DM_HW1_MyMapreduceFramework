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

config={}

if len (sys.argv) > 1:
    config_filename = sys.argv[1]
else:
    config_filename = default_config_file

try:
    with open(config_filename) as cfg:
        config = json.loads(cfg.read())
except FileExistsError:
    print("config file reading from {} problem".format(config_filename))


cmd = "example_word_counter_mapper.py data.txt worker_{}_"



start = time.perf_counter()



mappers=[]
for i in range(int (config['mappers_n']) ):
    cmd.format(i)
    mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "map_worker_{}_.txt".format(i)], stdout=PIPE, bufsize=1))

for pr in mappers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()


mappers_exit_codes = [p.wait() for p in mappers]


reducers_input_diapasone=[]

split_size = config['mappers_n'] // config['reducers_n']

last_diapasone_end = 0
for i in range(config['reducers_n']):
    if (i == config['reducers_n'] - 1):
        reducers_input_diapasone.append((last_diapasone_end, config['mappers_n']))
    else:
        reducers_input_diapasone.append((last_diapasone_end, last_diapasone_end+split_size))

    last_diapasone_end+= split_size


reducers = []
for i in range(config['reducers_n']):
    cmd.format(i)
    for diapasone in reducers_input_diapasone:
        for task_n in range(diapasone[0], diapasone[1]):
            from_file = "map_worker_{}_.txt".format(i)
            reducers.append(Popen([sys.executable, "-u", "example_word_counter_reducer.py", from_file, "result_reducer_{}_".format(task_n)],
                              stdout=PIPE, bufsize=1))

for pr in reducers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()


mappers_exit_codes = [p.wait() for p in reducers]




end = time.perf_counter()

print ("launcher", 1000000 * (end - start) , ' msec')