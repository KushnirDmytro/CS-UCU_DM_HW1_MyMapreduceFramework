# implementation of reducer interface for task
import sys
import operator
import time


def reduce (input_string_proxy):

    print ("reduce process started")
    start = time.perf_counter()

    local_dict = {}

    data = [line for line in input_string_proxy.splitlines()]
    print("data_before_reduce", len(data))

    for line in data:
        word, number = line.split(' :') #TODO list comprehansion
        number = int(number)
        if word in local_dict:
            local_dict[word] += number
        else:
            local_dict[word] = number


    end = time.perf_counter()

    print("reducing words vector time: ", end - start)
    return [list(local_dict.items())]
