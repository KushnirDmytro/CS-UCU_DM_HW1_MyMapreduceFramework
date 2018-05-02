# example of mapping task

import time
import sys
import re
import operator
import utils


def map(input_string_proxy):

    start = time.perf_counter()

    regEx = re.compile(r'[a-z0-9]+')
    local_list = [] #TODO rename

    data = [''.join(regEx.findall(word.lower())) for word in input_string_proxy.split()]
    print("data_been_red", len(data))

    for word in data:
        local_list.append((word, '1')) #TODO list comprehansion

    # using this because direct writing into proxy list is sooooooo slow...

    end = time.perf_counter()
    print ("wordscounter: ", end - start)
    return [local_list]  #for preserving list of lists return interface (multifile output)