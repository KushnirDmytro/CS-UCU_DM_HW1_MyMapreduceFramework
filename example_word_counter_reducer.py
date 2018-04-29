# implementation of reducer interface for task
import sys
import operator

def reduce (input_string_proxy, result_list):

    print ("reduce process started")

    local_dict = {}


    data = [line for line in input_string_proxy.value.splitlines()]
    print("data_before_reduce", len(data))

    i = 100000
    j = 0
    for line in data:
        word, number = line.split(' :')
        number = int(number)
        if word in local_dict:
            local_dict[word] += number
        else:
            local_dict[word] = number

        i -= 1
        if i == 0:
            j += 1
            i = 100000
            print("iter ", i * j)

    to_list = list(local_dict.items())

    print(to_list[0][1])

    # print(local_dict)

    print (to_list[:100])


    print (to_list[-100:])


    result_list.extend( to_list )

