# implementation of reducer interface for task
import sys
import operator
import time


def shuffle (input_string_proxy,  consumers_number):
    result_list = []
    print ("shuffling process started")
    start = time.perf_counter()

    for i in range (consumers_number):
        result_list.append([])

    data = [line for line in input_string_proxy.splitlines()]
    print("data_before_reduce", len(data))


    for line in data:
        word, number = line.split(' :')
        number = int(number)

        #adding to some list in lists according to hash value
        result_list[hash(word)%consumers_number].append( (word, number) ) #TODO list comprehansion if possible

    writing_header_tuple = ("output_files", len(result_list))

    result_list.insert(0,writing_header_tuple)

    end = time.perf_counter()

    print("reducing words vector time: ", end - start)

    return result_list