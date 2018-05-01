# implementation of reducer interface for task
import sys
import operator
import time


def shuffle (input_string_proxy, result_list, consumers_number):

    print ("shuffling process started")
    start = time.perf_counter()



    local_tuples_lists_list = [] #to do it faster
    for i in range (consumers_number):
        local_tuples_lists_list.append([])

    data = [line for line in input_string_proxy.value.splitlines()]
    print("data_before_reduce", len(data))



    for line in data:
        word, number = line.split(' :')
        number = int(number)

        #adding to some list in lists according to hash value
        local_tuples_lists_list[hash(word)%consumers_number].append( (word, number) )


    result_list.append(("output_files", len(local_tuples_lists_list)))
    result_list.extend( local_tuples_lists_list )

    end = time.perf_counter()

    print("reducing words vector time: ", end - start)
