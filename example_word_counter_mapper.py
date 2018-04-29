# example of mapping task

import time
import sys
import re
import operator
import utils


def h():
    print("Hello MAP!!!")

def map(input_string_proxy, result):


    start = time.perf_counter()


    # list_of_lines = input_string.split() #utils.read_from_raw_txt(filename)


    regEx = re.compile(r'[a-z0-9]+')

    data = [''.join(regEx.findall(word.lower())) for word in input_string_proxy.value.split()]
    print("data_been_red", len(data))
    print (data[:200])


    local_list = []

    for word in data:
        local_list.append((word, '1'))

    # using this because direct writing into proxy list is sooooooo slow...
    result.extend(local_list)

    end = time.perf_counter()

    print ("wordscounter: ", end - start)
    # return list_of_tuples



# data_file = sys.argv[1]
# print ("data_File ", data_file)
#
# out_file_name = sys.argv[2]
# print ("out_file_name ", out_file_name)
#
#
# resulting_list_of_tuples = map(filename=data_file)




# def map(filename, diapasone=()):
#     start = time.perf_counter()
#
#
#     list_of_lines = utils.read_from_raw_txt(filename)
#
#
#     regEx = re.compile(r'[a-zA-Z0-9]+')
#     data = [word.lower() for line in list_of_lines for word in regEx.findall(line)]
#     print("data_been_red", len(data))
#
#
#     list_of_tuples = []
#
#     for word in data:
#         list_of_tuples.append((word, 1))
#
#     end = time.perf_counter()
#
#     print ("wordscounter: ", end - start)
#     return list_of_tuples
#
#
#
# data_file = sys.argv[1]
# print ("data_File ", data_file)
#
# out_file_name = sys.argv[2]
# print ("out_file_name ", out_file_name)
#
#
# resulting_list_of_tuples = map(filename=data_file)
#
#
#
# try:
#     with open(out_file_name, 'w') as myfile:
#         for tuple in resulting_list_of_tuples:
#             myfile.write("%s : %d \n" % (tuple[0], tuple[1]))
# except FileExistsError:
#     print('file {} problem'.format(out_file_name))

