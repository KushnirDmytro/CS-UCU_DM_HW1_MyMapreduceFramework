import sys
import os
import time
from subprocess import Popen, PIPE

#TODO would be great to implement "Star map-reduce" algorithm
#TODO one more example for mapreduce

#TODO deal with python errors (via tuttorial)

#TODO file chunking for mapper task (or should it be separate, like resource manager....)

from MapReduceManeger import MapReduceManeger

help_msg = "This program demonstrates workflow of the mapreduce framework"
if ('-h' in sys.argv or '--help' in sys.argv):
    print (help_msg)


cmd = "example_word_counter_mapper.py data.txt worker_{}_"



mas = MapReduceManeger()

empty_task_config = {
    'type':'',
    'name':'',
    'executable_dir':'',
    'input_file':'' ,
    'output_file':''
}

# test = './mapping_result/map_test1_out.txt'
# with open(test, 'w') as myfile:
#     myfile.write("test")



mapper_task_config = {
    'type':'map',
    'name':'test1',
    'executable_dir':'example_word_counter_mapper.py',
    'input_file':'data.txt' ,
    'output_file':'./mapping_result/map_{}_out.txt'
}

reducer_task_config = {
    'type':'reduce',
    'name':'test1',
    'executable_dir':'example_word_counter_reducer.py',
    'input_file':'./mapping_result/map_test1_out.txt' ,
    'output_file':'./reduce_result/reduce_{}_out.txt'
}

mas.add_task(task_config=mapper_task_config)

mas.run()

mas.add_task(task_config=reducer_task_config)

mas.run()

#TODO GLOBAL!!!
# TODO make that subprocess got kind of pipeline to execute -> reader, mapper, writer
# TODO therefore possible to change to mapper -> writer -> combiner ->  -> writer
