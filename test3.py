import multiprocessing
import ctypes
import importlib
import os
# cstring = multiprocessing.Value(ctypes.c_char_p, "Hello, World!")

#
# testName = "example_word_counter_mapper"
#
#
# mod = importlib.import_module(testName)
# mod.h()
#
#
#
#
#
#
#
# def a(ar):
#     lst = []
#     lst.append(2)
#     ar = lst.copy()
#     # ar.append(1)
#     # print (ar.value)
#
# man = multiprocessing.Manager()
#
# str = man.list()
#
# str.append('a')
#
# pr = multiprocessing.Process(target=a, args=(str,))
#
# pr.start()
# pr.join()
#
# print(str)

filename = "data.txt"


start_index = 11964000
end_index = 11964700

with open(filename, 'r') as fin:

    print (os.stat(filename).st_size)
    fin.seek(start_index)
    first_line = fin.readline()
    print ("first_line", first_line)
    start_index = fin.tell()

    data = fin.read(end_index - start_index) + fin.readline()
    print(data)
# print(cstring)