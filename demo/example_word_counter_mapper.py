# example of mapping task

import time
import sys
import re
import operator

def map(filename, args_from=0, args_to=None):
    start = time.perf_counter()

    with open(filename, 'r') as myfile:
        regEx = re.compile(r'[a-zA-Z0-9]+')
        data = [word.lower() for line in myfile for word in regEx.findall(line)]
        print("data_been_red", len(data))

    list_of_tuples = []

    for word in data:
        #TODO clean trash from words (letter and symbols invariant)
        list_of_tuples.append((word, 1))

    end = time.perf_counter()

    print ("wordscounter: ", end - start)
    return list_of_tuples



data_file = sys.argv[1]
print ("data_File ", data_file)

worker_name = sys.argv[2]
print ("worker_name ", worker_name)

print ("HELLO SUBROC ={}=!!".format(worker_name))


resulting_list_of_tuples = map(filename=data_file)


out_file_name = "res_{}.txt".format(worker_name)

with open(out_file_name, 'w') as myfile:
    for tuple in resulting_list_of_tuples:
        myfile.write("%s : %d \n" % (tuple[0], tuple[1]))