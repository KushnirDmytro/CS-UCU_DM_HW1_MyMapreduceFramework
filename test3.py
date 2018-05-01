import multiprocessing
# import ctypes
# import importlib
#
# # cstring = multiprocessing.Value(ctypes.c_char_p, "Hello, World!")
#
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
def a(ar):
    lst = []

    popped = ar['a'].pop()
    ar['a'] = ar['a'][:-1]
     # = buf
    # lst = ar['a'][:-1]
    # print (lst)
    # print (popped)
    # ar = lst.copy()
    # ar.append(1)
    # print (ar.value)

man = multiprocessing.Manager()

str = man.dict()

str["a"] = ['a', 2]

pr = multiprocessing.Process(target=a, args=(str,))

pr.start()
pr.join()

print(str)

# print(cstring)

filename = 'data_txt'

