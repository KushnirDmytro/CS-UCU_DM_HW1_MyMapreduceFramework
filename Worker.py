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

    worker_status = ['active', 'waiting_resource', 'idle', 'error', 'dead' ]

    def __init__(self, ID):
        self.ID=ID
        self.task = None
        self.status = "idle"
        self.process = None
        self.data_manager = DataManager(self.ID)

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

    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        try:

            readed_data = ""
            data_source = self.task.read_from
            #Aquire data

            ##########

            #launch task

            #########

            #write result

            #########

            #release data
            #########

            self.set_status('active')
            self.task.status = 'active'


            resource_maneger = multiprocessing.Manager()
            data_from_file_holder = resource_maneger.Value(ctypes.c_char_p, "")


            pr = multiprocessing.Process(target=self.data_manager.read_file, args=(self.task.read_from,data_from_file_holder,))
            pr.start()
            pr.join()

            print("stop")
            print (len(data_from_file_holder.value))

            #TODO reuse same process in whole line (or concatenate pipeline and pass it to one)

            #instantiating executsable
            #TODO do it before and check API implementation

            """
            if hasattr(socket, 'fromfd'):
    pass
else:
    pass
            """

            mapping_holder = resource_maneger.list()


            print(self.task.executable)
            executable_module = importlib.import_module(self.task.executable)


            # executable_module.h()

            #TODO check types of a task to seek appropriate methods

            executable = multiprocessing.Process(target=executable_module.map, args=(data_from_file_holder,mapping_holder))

            executable.start()
            executable.join()

            print (len(mapping_holder) )

            output_filename = self.task.write_to.format(self.task.ID)

            writer_process =  multiprocessing.Process(target=self.data_manager.write_file, args=( output_filename,
                                                                                                  mapping_holder))

            writer_process.start()
            writer_process.join()




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

