# implementation of reducer interface for task
import sys
import operator
import time


def reduce (input_string_proxy, result_list):

    print ("reduce process started")
    start = time.perf_counter()

    local_dict = {}

    data = [line for line in input_string_proxy.value.splitlines()]
    print("data_before_reduce", len(data))

    for line in data:
        word, number = line.split(' :')
        number = int(number)
        if word in local_dict:
            local_dict[word] += number
        else:
            local_dict[word] = number


    to_list = local_dict.items()

    result_list.extend( [to_list] )

    end = time.perf_counter()

    print("reducing words vector time: ", end - start)
