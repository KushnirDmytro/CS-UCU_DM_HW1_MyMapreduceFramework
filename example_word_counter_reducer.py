# implementation of reducer interface for task
import sys
import operator

def reduce (filename, diapasone=()):

    dict = {}
    with open(filename, 'r') as myfile:

        data = [line for line in myfile]
        print("data_before_reduce", len(data))

        i = 100000
        j = 0
        for line in data:
            word, number = line.split(' :')
            number = int(number)
            if word in dict:
                dict[word]+= number
            else:
                dict[word] = number

            i-=1
            if i == 0:
                j+=1
                i=100000
                print("iter ", i*j)
        # print (data)

    return dict


data_file = sys.argv[1]
print ("data_File ", data_file)

out_file_name = sys.argv[2]
print ("out_file_name ", out_file_name)



resulting_dict_of_tuples = reduce(filename=data_file)




try:
    with open(out_file_name, 'w') as myfile:
        sorted_data = sorted(resulting_dict_of_tuples.items(), reverse=True,  key=operator.itemgetter(1))
        for key, value in sorted_data:
            myfile.write("%s : %d \n" % (key, value) )
except FileExistsError:
    print('file {} problem'.format(out_file_name))



#
# with open(out_file_name, 'w') as myfile:
#     for tuple in resulting_list_of_tuples:
#         myfile.write("%s : %d \n" % (tuple[0], tuple[1]))

