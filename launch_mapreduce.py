import sys
import os
import time
from subprocess import Popen, PIPE

from MapReduceManeger import MapReduceManeger



if ('-h' in sys.argv or '--help' in sys.argv):
    print (help_msg)

cmd = "example_word_counter_mapper.py data.txt worker_{}_"



mas = MapReduceManeger()






mappers=[]
for i in range(1):
    cmd.format(i)
    mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "worker_{}_".format(i)], stdout=PIPE, bufsize=1))

for pr in mappers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()


exit_codes = [p.wait() for p in mappers]

reducers = []


# cmd_reducer= "example_word_counter_mapper.py data.txt worker_{}_"

for i in range(1):
    cmd.format(i)
    reducers.append(Popen([sys.executable, "-u", "example_word_counter_reducer.py", "res_worker_0_.txt", "reducer_{}_".format(i)], stdout=PIPE, bufsize=1))

for pr in reducers:
    for line in iter(pr.stdout.readline, b''):
        print(line)
    pr.communicate()

for pr in reducers:
    pr.wait()

#
# for line in iter(proc[1].stdout.readline, b''):
#     print (line),
# proc[1].communicate()

# process1 = subprocess.Popen(
#     cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
# )

# while True:
#     out = process1.stdout.read(1)
#     if out == '' and process1.poll() != None:
#         break
#     print (out)
    # if out != '':
    #     sys.stdout.write(out)
    #     sys.stdout.flush()

# print(result)

start = time.perf_counter()

end = time.perf_counter()
print (type(end))
print ("launcher", 1000000 * (end - start) , ' Millisec')