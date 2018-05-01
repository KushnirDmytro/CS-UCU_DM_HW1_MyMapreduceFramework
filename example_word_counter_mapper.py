# example of mapping task

import time
import sys
import re
import operator
import utils


def map(input_string_proxy, result):

    start = time.perf_counter()

    regEx = re.compile(r'[a-z0-9]+')
    local_list = []

    data = [''.join(regEx.findall(word.lower())) for word in input_string_proxy.value.split()]
    print("data_been_red", len(data))

    for word in data:
        local_list.append((word, '1'))

    # using this because direct writing into proxy list is sooooooo slow...
    result.extend([local_list])

    end = time.perf_counter()
    print ("wordscounter: ", end - start)
