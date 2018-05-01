# Responsible for working with task, performing reduce or map itself, reads datafile writes output to
# other textfiles.

from Task import Task
from DataManager import DataManager
import subprocess
import multiprocessing
# from subprocess import Popen, PIPE
import sys
import ctypes
import importlib

class Worker:

    worker_status = ['active', 'waiting_resource', 'idle', 'error', 'dead', 'finished' ]

    def __init__(self, ID, dataManager, pipelineDict):
        self.ID=ID
        self.task = None
        self.status = "idle"
        self.process = None
        self.data_manager = dataManager
        self.function_to_call = None
        self.pipeline = pipelineDict


        print ("Hello from worker [{}] status [{}] task [{}]".format(self.ID, self.status, self.task))


    # def read_from(self, filename, diapasone = None):
    #     """
    #     As we presume immutability of reading file, no need to prevent datarace
    #     :param filename: name of file with inputs
    #     :return: string of file content
    #     """
    #     # todo move to DM
    #     #todo add .csv
    #     pass


    def is_idle (self):
        return self.status == 'idle'


    def set_status(self, status):
        if status in self.worker_status:
            self.status = status
        else:
            raise ValueError(" [{}] is unsupported status value for worker {}".format(status, self.ID))


    def set_task(self, task):
        if self.is_idle:
            self.task = task

        try:
            self.function_to_call = self.task.get_executable_function()
        except Exception:
            print("Task initialisation conflict from Worker {}".format(self.ID))

    # config = {
    #     "task_type": type,
    #     'ID': str(id),
    #     'executable_dir': 'example_word_counter_mapper',
    #     'input_src': input_config,
    #     'output_files_template': out_template
    # }

    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        try:

            #Aquire data

            #TODO make iterationd over src list and launch this worker for all of them

            self.set_status('waiting_resource')
            self.task.status = 'active'


            this_task_type = self.task.config.task_type
            next_step_task = self.pipeline[this_task_type]

            input_data_source = self.task.config.input_src

            print (input_data_source)

            input_files = input_data_source.files
            input_partitions = input_data_source.partitions

            data_monitor = self.data_manager.available_data_monitor
            resource_maneger = self.data_manager.shared_data_manager


            input_string_proxy = resource_maneger.Value(ctypes.c_char_p, "")

            # TODO customise reading (monitor the case of raw txt input)

            reader_function = self.data_manager.read_input_files


            pr = multiprocessing.Process(target=reader_function,
                                         args=(input_files, input_partitions , input_string_proxy,))

            pr.start()
            pr.join()

            print("DATA AQUIRED:")
            print(len(input_string_proxy.value))

            ##########


            #launch task

            self.set_status('active')
            self.task.status = 'active' #todo setter!

            result_tuple_list_proxy = resource_maneger.list()

            task_args = (input_string_proxy, result_tuple_list_proxy)

            if this_task_type == 'shuffle': #TODO yes, I know it is bad, but arcitecture is my weak spot
                task_args = (input_string_proxy, result_tuple_list_proxy,
                             int(self.data_manager.master_config['active_reducers_up_to']))

            executable = multiprocessing.Process(target=self.function_to_call,
                                                 args=task_args)

            executable.start()
            executable.join()

            print('mapping elements returned :', str(len(result_tuple_list_proxy)))
            #########

            #clear memory
            input_string_proxy = None

            #write result

            self.set_status('waiting_resource')
            self.task.status = 'active'

            output_flag = "out"
            #TODO configure to support writing to multiple directories
            #TODO template_filling_only_here


            number_of_output_files = 1
            if (result_tuple_list_proxy[0][0] == 'output_files'):
                number_of_output_files = result_tuple_list_proxy[0][1]
                result_tuple_list_proxy = result_tuple_list_proxy[1:]

            for output_file_index in range ( number_of_output_files ):#different outputs to different files

                output_filename = self.data_manager.build_template_for_output_data_file (
                    type = this_task_type, flag=output_flag, id=self.ID, input=0, output=output_file_index, file_ext='txt'
                )

                print("wrinting to ", output_filename)

                writer_process = multiprocessing.Process(
                    target=self.data_manager.write_file,
                    args=(output_filename,result_tuple_list_proxy[output_file_index])
                )

                writer_process.start()
                writer_process.join()

                print("wrinting to ", output_filename, " DONE")



                print("WAS data_available[{}]".format(next_step_task) )
                print(data_monitor[next_step_task])

                data_monitor[next_step_task] += [output_filename] #adding to available data new resource

                print("NOW data_available[{}]".format(next_step_task))
                print (data_monitor[next_step_task])

            #########

            #release data
            result_tuple_list_proxy = None

            self.set_status('finished')
            self.task.status = 'finished'
            #########


            #TODO reuse same process in whole line (or concatenate pipeline and pass it to one)


            # process = subprocess.Process(
            #     [sys.executable,
            #      "-u",
            #      self.task.executable,
            #      self.task.read_from,
            #      self.task.write_to.format(self.task.ID)],
            #      stdout=subprocess.PIPE,
            #     bufsize=1)

            # todo manage notification when and how process ended

            # status = process.returncode
            #todo manage return code and administrate worker's status



        except ChildProcessError:
            #todo test if works
            raise ChildProcessError("failed to launch task {}__{}{} from worker {} ".format(self.task.executable,
                                                                                      self.task.name,
                                                                                      self.task.type,
                                                                                      self.ID))
        finally:
            pass #TODO





#
# start = time.perf_counter()
#
#
#
# mappers=[]
# for i in range(int (config['mappers_n']) ):
#     cmd.format(i)
#     mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "map_worker_{}_.txt".format(i)], stdout=PIPE, bufsize=1))

