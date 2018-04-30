class DataManager:
    """
    manages reading, writing, transition of data and memory_aquisition
    """


    def __init__(self, id):
        self.id_ = id
        self.mem_aquired = None


        print("HELLO INIT ResourceManager" + id)

        print("INIT OK ResourceManager" + id)


    def read_txt(self, filename):
        pass


    def read_csv(self, filename):
        pass

    def write_txt(self, filename):
        pass

    def write_csv(self, filename):
        pass

    def read_file(self, filename, read_to, diapasone=()):
        """
        reading policy used: read up to diapasone end + 1 line, then this line will be skipped in the next chunk
        """
        print("READING from {} ".format(filename))


        with open(filename, 'rt') as myfile:
            if diapasone == ():
                #reading whole file
                read_to.value = myfile.read()
                print("Been read " + str(len(read_to.value)))
            else:
                start_pos, end_pos = diapasone
                myfile.seek(start_pos)
                myfile.readline() #skipping to the endline position
                real_start_pos = myfile.tell()

                myfile.seek(end_pos)
                myfile.readline()
                real_end_pos = myfile.tell()
                # optimisation to avoid long strings creation and concatenetion
                read_to.value = myfile.read(real_end_pos-real_start_pos)

                #TODO check diapason availability (file size)
                pass
                #TODO read this part of file


    def write_file(self, out_file_name,  resulting_list_of_tuples ):
        """
        :param resulting_list_of_tuples: we assume that all pipeline share common representation of output form
        :param out_file_name:
        """
        try:
            with open(out_file_name, 'w') as myfile:

                i=1

                local_thread_buffer = resulting_list_of_tuples._getvalue() # for reading from proxy object speedup



                for tuple in local_thread_buffer:
                    if i == 1:
                        print(tuple)
                    myfile.write("%s : %s \n" % (tuple[0], tuple[1] ))
                    i+=1

        except FileExistsError:
            print('file {} problem'.format(out_file_name))
        pass