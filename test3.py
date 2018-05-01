import multiprocessing
import ctypes
import importlib
import os
#


testName = "example_word_counter_mapper"


# mod = importlib.import_module(testName)
# mod.h()
# #
# #
# #
# #
# #
# #
# #
def a(str):
    # lst = []
    #
    # str.value += os.linesep + "world!"
    str.append(1)
    #
    # popped = ar['a'].pop()
    # ar['a'] = ar['a'][:-1]
     # = buf
    # lst = ar['a'][:-1]
    # print (lst)
    # print (popped)
    # ar = lst.copy()
    # ar.append(1)
    # print (ar.value)

man = multiprocessing.Manager()

# str = man.dict()

cstring = man.Value(ctypes.c_char_p, "Hello, ")

prox_list = man.list()

# pr = multiprocessing.Process(target=a, args=(cstring,))
pr = multiprocessing.Process(target=a, args=(prox_list,))

pr.start()
pr.join()


print (prox_list)

# str["a"] = ['a', 2]


print(str)

# print(cstring)

# filename = 'data_txt'
#

#
# a = {"a":1}
# b = {"B":2}
# print (a.b)

a =  [1,2,3,4]
print (a[:2])

b = 'test'

hashed = hash(b) % 3

print (hashed)

d = {}

def b(a):
    a.append(0)

c = []
b (c)
print(c)



dct = man.dict()
dct ["a"] = man.list()


ptr = dct["a"]
ptr.extend([1,2])
dct["a"].pop()
print(type(dct["a"]))
print(dct['a'])