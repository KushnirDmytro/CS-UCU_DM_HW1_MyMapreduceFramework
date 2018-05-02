import sys
import os
import time
from subprocess import Popen, PIPE

#TODO would be great to implement "Star map-reduce" algorithm
#TODO one more example for mapreduce

#TODO requirements.txt !!!!

from MapReduceManager import MapReduceManager

help_msg = "This program demonstrates workflow of the mapreduce framework"
if ('-h' in sys.argv or '--help' in sys.argv):
    print (help_msg)


# cmd = "example_word_counter_mapper.py data.txt worker_{}_"

mas = MapReduceManager()



#TODO now it is external, but can be automated and encapsulated in M-R_manager

mappers_configs = mas.create_mappers_configs()
print(mappers_configs)

mas.spawn_task_from_config(task_config=mappers_configs[0])

mas.run()



