import os
import multiprocessing
import ctypes
#TODO extract reader and writer to utils class

class DataManager:
    """
    manages reading, writing, transition of data and memory_aquisition
    """


    def __init__(self, id, master_config_dict):
        self.id_ = id
        self.mem_aquired = None
        self.shared_data_manager = multiprocessing.Manager()
        self.master_config  = master_config_dict #to isolate but access info is easy
        self.available_data_monitor  = self.shared_data_manager.dict()
        self.available_data_monitor["map"] = self.shared_data_manager.list()
        self.available_data_monitor["reduce"] = self.shared_data_manager.list()
        self.available_data_monitor["combine"] = self.shared_data_manager.list()
        self.available_data_monitor["shuffle"] = self.shared_data_manager.list()
        self.available_data_monitor["finish"] = self.shared_data_manager.list()
        self.resource_available_flag = multiprocessing.Condition()





        print("HELLO INIT ResourceManager" + id)

        print("INIT OK ResourceManager" + id)


    def create_state_proxy(self, strVal):
        return self.shared_data_manager.Value(ctypes.c_char_p, strVal)

    def read_txt(self, filename):
        pass


    def read_csv(self, filename):
        pass

    def write_txt(self, filename):
        pass

    def write_csv(self, filename):
        pass


    def build_template_for_output_data_file (self, type, flag, id, input=0, output=0, file_ext='txt'):
        """
        :return: template that needs to instert id and flag of resulting process
        """

        pref = ""
        if (type == 'map'):
            pref = "./mapping_result/"
        if (type == 'reduce'):
            pref = "./reduce_result/"
        if (type == 'shuffle'):
            pref = "./shuffle_result/"
        if (type == 'combine'):
            pref = "./combine_result/"

        return (pref + "{}_{}_{}_{}_{}.{}".format(type, id, flag, input, output, file_ext ))


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



    def read_input_files(self, input_files, input_partitions):
        """
        reading policy used: read up to diapasone end + 1 line, then this line will be skipped in the next chunk
        """
        obtained_stings_list = []
        print("READING from GENERAL {} ".format(input_files))
        for input_file, input_file_partition in zip(input_files, input_partitions):

            print("READING from {} ".format(input_file))

            if input_file_partition == (1,1):
                #reading whole file
                with open(input_file, 'rt') as myfile:
                    obtained_stings_list.append(myfile.read())
                    print("Been read " + str(len(obtained_stings_list[len(obtained_stings_list)-1])))

            else:
                start_pos, end_pos = self.get_file_diapasone (input_file, input_file_partition)
                with open(input_file, 'rt') as myfile:
                    # optimisation to avoid long strings creation and concatenetion
                    myfile.seek(start_pos)
                    obtained_stings_list.append(myfile.read(end_pos-start_pos))


        print("reading_complete")
        return os.linesep.join(obtained_stings_list)



    def write_file(self, out_file_name,  resulting_list_of_tuples ):
        """
        :param resulting_list_of_tuples: we assume that all pipeline share common representation of output form
        :param out_file_name:
        """
        try:
            with open(out_file_name, 'w') as myfile:

                i=1

                local_thread_buffer = resulting_list_of_tuples # for reading from proxy object speedup

                for tuple in local_thread_buffer:
                    if i == 1:
                        print(tuple)
                    myfile.write("%s : %s \n" % (tuple[0], tuple[1] ))
                    i+=1

        except FileExistsError:
            print('file {} problem'.format(out_file_name))

    def has_available_data(self):
        #TODO lock this if use more than one manager
        result = False
        for data_class, value in self.available_data_monitor.items()[:-1]: #avoiding key "finished"
            if len(value) > 0:
                result = True
                break
        return result


    def get_available_task_and_data(self):
        available_data = None
        available_data_type = None

        for task_name, value in self.available_data_monitor.items()[:-1]:
            if len(value) > 0:
                available_data = [self.available_data_monitor[task_name][0]]
                self.available_data_monitor[task_name].pop()
                available_data_type = task_name
                break

        return available_data, available_data_type


