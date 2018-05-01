import os
import multiprocessing
#TODO extract reader and writer to utils class

class DataManager:
    """
    manages reading, writing, transition of data and memory_aquisition
    """


    def __init__(self, id):
        self.id_ = id
        self.mem_aquired = None
        self.shared_data_manager = multiprocessing.Manager()

        self.available_data_monitor  = self.shared_data_manager.dict()
        self.available_data_monitor["map"] = []
        self.available_data_monitor["reduce"] = []
        self.available_data_monitor["combine"] = []
        self.available_data_monitor["shuffle"] = []




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


    def get_file_diapasone (self, filename, partitioning):

        has_partitions, this_partition_number = partitioning

        file_size = os.stat(filename).st_size
        chunk_size = file_size // has_partitions

        if has_partitions == this_partition_number:
            end_pos = file_size
        else:
            end_pos = chunk_size * this_partition_number

        start_pos = end_pos - chunk_size

        with open(filename, 'rt') as myfile:

            myfile.seek(start_pos)
            myfile.readline()  # skipping to the endline position
            real_start_pos = myfile.tell()

            myfile.seek(end_pos)
            myfile.readline()
            real_end_pos = myfile.tell()
            #assume if we were at the EOF, no reading will happen

        return real_start_pos , real_end_pos



    def read_file(self, filename, partitioning, read_to ):
        """
        reading policy used: read up to diapasone end + 1 line, then this line will be skipped in the next chunk
        """
        print("READING from {} ".format(filename))

        if partitioning == (1,1):
            #reading whole file
            with open(filename, 'rt') as myfile:
                read_to.value = myfile.read()
                print("Been read " + str(len(read_to.value)))

        else:
            start_pos, end_pos = self.get_file_diapasone (filename, partitioning)
            with open(filename, 'rt') as myfile:
                # optimisation to avoid long strings creation and concatenetion
                myfile.seek(start_pos)
                read_to.value = myfile.read(end_pos-start_pos)




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