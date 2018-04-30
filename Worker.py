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

    def __init__(self, ID):
        self.ID=ID
        self.task = None
        self.status = "idle"
        self.process = None
        self.data_manager = DataManager(self.ID)
        self.executable_module = None
        self.function_to_call = None

        print ("Hello from worker {} status {} task {}".format(self.ID, self.status, self.task))



    def read_from(self, filename, diapasone = None):
        """
        As we presume immutability of reading file, no need to prevent datarace
        :param filename: name of file with inputs
        :return: string of file content
        """
        # todo move to DM
        #todo add .csv
        pass


    def is_idle (self):
        return self.status == 'idle'


    def set_status(self, status):
        if status in self.worker_status:
            self.status = status
        else:
            raise ValueError(" [{}] is unsupported status value for worker {}".format(status, self.ID))




    # def write_res_to(self, filename):
    #     pass

    def set_task(self, task):
        if self.is_idle:
            self.task = task

        try:
            self.function_to_call = self.task.get_executable_function()
        except Exception:
            print("Task initialisation conflict from Worker {}".format(self.ID))




    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        try:

            #Aquire data

            self.set_status('waiting_resource')
            self.task.status = 'active'
            data_source = self.task.read_from

            resource_maneger = multiprocessing.Manager()
            data_proxy = resource_maneger.Value(ctypes.c_char_p, "")

            # TODO customise reading (monitor the case of raw txt input) IDEA!!! use csv to save tuples!!!

            pr = multiprocessing.Process(target=self.data_manager.read_file, args=(data_source, data_proxy,))
            pr.start()
            pr.join()

            print("DATA AQUIRED:")
            print(len(data_proxy.value))

            ##########


            #launch task

            self.set_status('active')
            self.task.status = 'active'

            result_tuple_list_proxy = resource_maneger.list()

            print(self.task.executable)
            executable_module = importlib.import_module(self.task.executable)

            callable_method_name = self.task.task_type

            function_to_call = getattr(executable_module, callable_method_name)
            executable = multiprocessing.Process(target=function_to_call,
                                                 args=(data_proxy, result_tuple_list_proxy))

            executable.start()
            executable.join()

            print('mapping elements returned :', str(len(result_tuple_list_proxy)))
            #########

            #clear memory
            data_proxy = None

            #write result

            self.set_status('waiting_resource')
            self.task.status = 'active'

            output_filename = self.task.write_to.format(self.task.ID)

            print("wrinting to ", output_filename)

            writer_process = multiprocessing.Process(target=self.data_manager.write_file, args=(output_filename,
                                                                                                result_tuple_list_proxy))
            writer_process.start()
            writer_process.join()

            print("wrinting to ", output_filename, " DONE")
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



cmd = "example_word_counter_mapper.py data.txt worker_{}_"


#
# start = time.perf_counter()
#
#
#
# mappers=[]
# for i in range(int (config['mappers_n']) ):
#     cmd.format(i)
#     mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "map_worker_{}_.txt".format(i)], stdout=PIPE, bufsize=1))

