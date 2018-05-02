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
        self.process = None
        self.data_manager = dataManager
        self.status = dataManager.shared_data_manager.Value(ctypes.c_char_p, "idle") #TODO rename to Proxy
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
            self.status.value = status
        else:
            raise ValueError(" [{}] is unsupported status value for worker {}".format(status, self.ID))


    def set_task(self, task):
        if self.is_idle:
            self.task = task

        try:
            self.function_to_call = self.task.get_executable_function()
        except Exception:
            print("Task initialisation conflict from Worker {}".format(self.ID))


    def subprocess_execution(self,
                             this_task_type, next_task_type,
                             reader_fun, reader_args,
                             job_fun, job_args,
                             writer_fun, tamplater,
                             worker_state_proxy,
                             data_monitor
                             ):

        # TODO make iterationd over src list and launch this worker for all of them
        # self.task.set_status('active')

        worker_state_proxy.value = 'waiting_resource' #TODO out of subprocess


        input_string_proxy = reader_fun (reader_args[0], reader_args[1] )


        print("DATA AQUIRED:")
        print(len(input_string_proxy) )

        ##########

        # launch task

        worker_state_proxy.value = 'active'



        if this_task_type == 'shuffle':  # TODO yes, I know it is bad, but arcitecture is my weak spot
            job_rez = job_fun(input_string_proxy,
                         int(self.data_manager.master_config['active_reducers_up_to']))
        else:
            job_rez = job_fun(input_string_proxy)



        # executable = multiprocessing.Process(target=self.function_to_call,
        #                                      args=task_args)

        # executable.start()
        # executable.join()

        print('mapping elements returned :', str(len(job_rez)))
        #########




        output_flag = "out"

        number_of_output_files = 1
        if (job_rez[0][0] == 'output_files'):
            number_of_output_files = job_rez[0][1]
            job_rez = job_rez[1:]

        for output_file_index in range(number_of_output_files):  # different outputs to different files

            output_filename = tamplater(
                type=this_task_type, flag=output_flag, id=self.ID, input=0, output=output_file_index, file_ext='txt'
            )

            print("wrinting to ", output_filename)

            # writer_args = (output_filename, job_rez[output_file_index])

            writer_fun(output_filename, job_rez[output_file_index])

            print("wrinting to ", output_filename, " DONE")

            print("WAS data_available")
            print(data_monitor[next_task_type])

            data_monitor[next_task_type] += [output_filename]  # adding to available data new resource

            print("NOW data_available")
            print(data_monitor[next_task_type])

        #########

        # release data
        result_tuple_list_proxy = None

        worker_state_proxy.value = 'finished'




    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        try:

            #Prepare sync
            this_task_type = self.task.config.task_type
            next_step_task = self.pipeline[this_task_type]

            input_data_source = self.task.config.input_src

            print(input_data_source)

            input_files = input_data_source.files
            input_partitions = input_data_source.partitions

            data_monitor = self.data_manager.available_data_monitor
            resource_maneger = self.data_manager.shared_data_manager

            input_string_proxy = resource_maneger.Value(ctypes.c_char_p, "")

            reader_function = self.data_manager.read_input_files
            reader_args = (input_files, input_partitions, input_string_proxy,)

            result_tuple_list_proxy = resource_maneger.list()

            task_args = (input_string_proxy, result_tuple_list_proxy)

            writer_fn = self.data_manager.write_file

            tamplater = self.data_manager.build_template_for_output_data_file

            #Aquire data

            # def subprocess_execution(self,
            #                              this_task_type,
            #                              reader_fun, reader_args,
            #                              job_fun, job_args,
            #                              writer_fun, tamplater,
            #                              worker_state_proxy,
            #                              data_monitor
            #                              ):
            if this_task_type == 'shuffle':  # TODO yes, I know it is bad, but arcitecture is my weak spot
                task_args = (input_string_proxy, result_tuple_list_proxy,
                             int(self.data_manager.master_config['active_reducers_up_to']))

            self.subprocess_execution(
                this_task_type=this_task_type,
                next_task_type= next_step_task,
                reader_fun=reader_function,
                reader_args=reader_args,
                job_fun=self.function_to_call,
                job_args=task_args,
                writer_fun=writer_fn,
                tamplater=tamplater,
                worker_state_proxy=self.status,
                data_monitor=data_monitor
            )



            #
            #
            # #TODO make iterationd over src list and launch this worker for all of them
            # self.task.set_status('active')
            #
            # self.set_status('waiting_resource')
            #
            #
            # pr = multiprocessing.Process(target=reader_function,
            #                              args=reader_args)
            #
            # pr.start()
            # pr.join()
            #
            # print("DATA AQUIRED:")
            # print(len(input_string_proxy.value))
            #
            # ##########
            #
            #
            # #launch task
            #
            # self.set_status('active')
            #
            #
            #
            #
            #
            #
            # executable = multiprocessing.Process(target=self.function_to_call,
            #                                      args=task_args)
            #
            # executable.start()
            # executable.join()
            #
            # print('mapping elements returned :', str(len(result_tuple_list_proxy)))
            # #########
            #
            # #clear memory
            # input_string_proxy = None
            #
            # #write result
            #
            # self.set_status('waiting_resource')
            #
            #
            # output_flag = "out"
            #
            #
            # number_of_output_files = 1
            # if (result_tuple_list_proxy[0][0] == 'output_files'):
            #     number_of_output_files = result_tuple_list_proxy[0][1]
            #     result_tuple_list_proxy = result_tuple_list_proxy[1:]
            #
            # for output_file_index in range ( number_of_output_files ):#different outputs to different files
            #
            #     output_filename = tamplater (
            #         type = this_task_type, flag=output_flag, id=self.ID, input=0, output=output_file_index, file_ext='txt'
            #     )
            #
            #     print("wrinting to ", output_filename)
            #
            #
            #     writer_args = (output_filename,result_tuple_list_proxy[output_file_index])
            #
            #     writer_process = multiprocessing.Process(
            #         target=writer_fn,
            #         args=writer_args
            #     )
            #
            #     writer_process.start()
            #     writer_process.join()
            #
            #     print("wrinting to ", output_filename, " DONE")
            #
            #
            #
            #     print("WAS data_available[{}]".format(next_step_task) )
            #     print(data_monitor)
            #
            #     data_monitor += [output_filename] #adding to available data new resource
            #
            #     print("NOW data_available[{}]".format(next_step_task))
            #     print (data_monitor)
            #
            # #########
            #
            # #release data
            # result_tuple_list_proxy = None
            #
            # self.set_status('finished')
            #
            #
            # self.task.status = 'finished'
            # #########
            #
            #
            # #TODO reuse same process in whole line (or concatenate pipeline and pass it to one)
            #
            #
            # # process = subprocess.Process(
            # #     [sys.executable,
            # #      "-u",
            # #      self.task.executable,
            # #      self.task.read_from,
            # #      self.task.write_to.format(self.task.ID)],
            # #      stdout=subprocess.PIPE,
            # #     bufsize=1)
            #
            # # todo manage notification when and how process ended
            #
            # # status = process.returncode
            # #todo manage return code and administrate worker's status



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

