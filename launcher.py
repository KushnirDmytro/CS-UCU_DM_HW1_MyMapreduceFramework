import sys
import os
import time
from subprocess import Popen, PIPE

#TODO would be great to implement "Star map-reduce" algorithm
#TODO one more example for mapreduce

#TODO requirements.txt !!!!

#TODO deal with python errors (via tuttorial)

#TODO file chunking for mapper task (or should it be separate, like resource manager....)

from MapReduceManager import MapReduceManager

help_msg = "This program demonstrates workflow of the mapreduce framework"
if ('-h' in sys.argv or '--help' in sys.argv):
    print (help_msg)


cmd = "example_word_counter_mapper.py data.txt worker_{}_"



mas = MapReduceManager()

empty_task_config = {
    'task_type':'',
    'ID':'',
    'executable_dir':'',
    'input_files':'' ,
    'output_files':''
}

# test = './mapping_result/map_test1_out.txt'
# with open(test, 'w') as myfile:
#     myfile.write("test")



reducer_task_config = {
    'task_type':'reduce',
    'ID':'1',
    'executable_dir':'example_word_counter_reducer',
    'input_files':['./mapping_result/map_1_out.txt'] ,
    'output_files':['./reduce_result/reduce_{}_out_new.txt'],
    'brothers':1
}

#TODO now it is external, but must be automated and encapsulated in M-R_manager

mappers_configs = mas.create_mappers_configs()
print(mappers_configs)



mas.add_task(task_config=mappers_configs[0])

mas.run()

mas.add_task(task_config=mappers_configs[1])

mas.run()

mas.add_task(task_config=reducer_task_config)

mas.run()

#TODO GLOBAL!!!
# TODO make that subprocess got kind of pipeline to execute -> reader, mapper, writer
# TODO therefore possible to change to mapper -> writer -> combiner ->  -> writer
