# Responsible for working with task, performing reduce or map itself, reads datafile writes output to
# other textfiles.

from Task import Task
from DataManager import DataManager
import subprocess
import multiprocessing
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


    def execute_in_subprocess(self,
                              this_task_type, next_task_type,
                              reader_fun, reader_args,
                              job_fun,
                              writer_fun, tamplater,

                              worker_state_proxy, task_state_proxy,

                              data_monitor,
                              resource_available_flag
                              ):


        worker_state_proxy.value = 'waiting_resource'
        task_state_proxy.value = 'active'

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

            writer_fun(output_filename, job_rez[output_file_index])

            print("wrinting to ", output_filename, " DONE")
            # release data

            with resource_available_flag:
                # print("WAS [{}] data_available".format(next_task_type))
                # print(data_monitor[next_task_type])
                data_monitor[next_task_type] += [output_filename]  # adding to available data new resource

                print("NOW [{}] data_available".format(next_task_type) )
                print(data_monitor[next_task_type])

                resource_available_flag.notify_all()

        worker_state_proxy.value = 'finished'
        task_state_proxy.value = 'finished'




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

            input_files = input_data_source.files
            input_partitions = input_data_source.partitions

            data_monitor = self.data_manager.available_data_monitor
            resource_maneger = self.data_manager.shared_data_manager

            input_string_proxy = resource_maneger.Value(ctypes.c_char_p, "")

            reader_function = self.data_manager.read_input_files
            reader_args = (input_files, input_partitions, input_string_proxy,)


            writer_fn = self.data_manager.write_file

            tamplater = self.data_manager.build_template_for_output_data_file

            task_status_proxy = self.task.status

            #Aquire data




            fork_args = (this_task_type,
                         next_step_task,
                        reader_function,
                        reader_args,
                        self.function_to_call,
                        writer_fn,
                        tamplater,
                        self.status,  task_status_proxy,
                        data_monitor,
                         self.data_manager.resource_available_flag )

            fork_executor =  self.execute_in_subprocess

            print()
            print()
            print()
            print()

            forked_worker = multiprocessing.Process(target=fork_executor,
                                                                         args=fork_args)


            forked_worker.start()

            # #TODO reuse same process in whole line (or concatenate pipeline and pass it to one)
            # #todo manage return code and administrate worker's status



        except ChildProcessError:
            raise ChildProcessError("failed to launch task {}__{}{} from worker {} ".format(self.task.executable,
                                                                                      self.task.name,
                                                                                      self.task.type,
                                                                                      self.ID))
        finally:
            pass #TODO


