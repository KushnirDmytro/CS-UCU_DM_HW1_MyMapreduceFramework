# class manages over operating nodes, creates processes, checks it's activity etc.

import sys
import os
import json
from Task import Task
from Worker import Worker
from DataManager import DataManager
import multiprocessing





class MapReduceManager:
    """
    We don't need special storage for ready data, because they are output directory names for each finished task
    """

    def craete_task_pipeline_scenario(self):
        """
        According to configured mode creates scenario of processing data (which steps to use)
        """
        pipeline = {"reduce":"finish"}
        if self.config_dict['use_combiners'] == "True":
            pipeline['map'] = 'combine'
            if self.config_dict['active_reducers_up_to'] > 1:
                pipeline['combine'] = 'shuffle'
                pipeline['shuffle'] = 'reduce'
            else:
                pipeline['combine'] = 'reduce'

        else:
            if self.config_dict['active_reducers_up_to'] > 1:
                pipeline['map'] = 'shuffle'
                pipeline['shuffle'] = 'reduce'

            else:
                pipeline['map'] = 'reduce'
        return pipeline



    def __init__(self, config_filename="config.json"):
        print("hello from MasteNode1")

        self.read_config(config_filename)

        self.task_types = Task.supported_types
        print ("Available Task types", self.task_types)


        self.workers = {}
        self.tasks = {}
        for task_type in self.task_types:
            self.workers[task_type] = [] #holders for workers and tasks
            self.tasks[task_type] = []


        self.help_msg = "" #<== TODO <maybe clean

        self.last_worker_created_ID = 0
        self.last_task_created_ID = 0

        self.data_manager = DataManager("1", self.config_dict) #as ID
        self.pipeline_dict = self.craete_task_pipeline_scenario()


        #TODO add flag for 'time to die'


    def read_config(self, config_filename):
        # todo check if config is ok values
        # todo mode for csv.files ==> Data manager
        try:
            with open(config_filename) as cfg:
                self.config_dict =  json.loads(cfg.read())
        except FileExistsError:
            print("config file reading from {} problem".format(config_filename))



    # mapper_task1_config = {
    #     'task_type': 'map',
    #     'ID': '1',
    #     'executable_dir': 'example_word_counter_mapper',
    #     'input_files': ['data.txt'],
    #     'output_files': ['./mapping_result/map_{}_out.txt'],
    #     'readind_diapasones_list':[()]
    # }
    #
    # {
    #     "active_mappers_up_to": 2,
    #     "active_reducers_up_to": 1,
    #     "active_combiners_up_to": 0,
    #     "active_shufflers_up_to": 0,
    #     "total_workers_number": 3,
    #     "data_sources": ["data.txt"],
    #     "mapper_file_name": "example_word_counter_mapper.py",
    #     "reducer_file_name": "example_word_counter_reducer.py",
    #     "memory_limit_total": "",
    #     "memory_limit_per_process": ""
    # }

    def make_reading_diapasones(self, files_list, consumers_n):

        # input_splits_list_example = [{
        #     "files":["d1", 'd2'],
        #     "partitions":[(1,1), (2,1) ]
        # }]

        input_splits_list = []
        files_n = len(files_list)
        if consumers_n >= files_n :
            consumers_at_least = consumers_n // files_n #at_most = at_least+1
            files_with_additional_consumer = consumers_n % files_n
            files_without_aditional_consumer = files_n - files_with_additional_consumer

            overall_conusmer_id = 0

            for file_indx in range(files_n):
                file_name = files_list[file_indx]
                this_file_consumers = consumers_at_least
                if file_indx + 1 > files_without_aditional_consumer:
                    this_file_consumers += 1
                for this_file_consumer_indx in range(this_file_consumers):
                    input_splits_list.append(
                        {
                            "files": [ file_name ],
                            "partitions":[ (this_file_consumers, this_file_consumer_indx+1) ]
                        }
                        # use natural numbers indexation to avoid (0,0) case
                    )
                    overall_conusmer_id+=1

        else:

            #more files then  consumers
            #in this case we'll use file-by-file processing approach (reading from quiue) instead of indexing
            pass #TODO refactor it out (I've planned bigger function instead)

        return input_splits_list


    def build_task_config(self, type, id, input_config):
        config =  {
                    "task_type": type,
                    'ID': str(id),
                    'executable_dir': self.config_dict['executables'][type],
                    'input_src':input_config
                }
        return config

    def create_mappers_configs(self):
        mappers = self.config_dict['active_mappers_up_to']
        mappers_task_list = []

        diapasones_list  = self.make_reading_diapasones (self.config_dict['data_sources'], mappers)

        task_id = 0
        for diapasone in diapasones_list:
            mappers_task_list.append  (
                self.build_task_config (type='map',
                                        id = task_id,
                                        input_config=diapasone,
                                        )
            )
            task_id+=1

        return mappers_task_list




    def spawn_task_from_config(self, task_config):

        task_state_proxy = self.data_manager.create_state_proxy("idle")
        new_task = Task(task_config, task_state_proxy)
        self.tasks[new_task.config.task_type].append(new_task)

    def get_idle_task(self):
        founded_task = None
        for task_type in self.tasks:
            for task in self.tasks[task_type]:
                if task.is_idle():
                    founded_task = task
        return founded_task




    def run(self):

        while (True):
            with self.data_manager.resource_available_flag:
                if (not self.data_manager.has_available_data() is None and (self.get_idle_task() is  None) ):
                    self.data_manager.resource_available_flag.wait()

                available_data, task_type = self.data_manager.get_available_task_and_data()


                if not available_data is None:
                    new_task_config = self.build_task_config(
                        type=task_type,
                        id=self.last_worker_created_ID,
                        input_config={
                            "files": available_data,
                            "partitions": [(1, 1)] #todo implement this feature
                        },
                    )

                    self.last_task_created_ID += 1
                    self.spawn_task_from_config(new_task_config)


                # TODO data extraction
                # TODO task registration here

                else:
                    idle_task = self.get_idle_task()

                    if not idle_task is None and idle_task.is_idle(): #double check
                        new_worker = self.spawn_worker(
                            idle_task)  # TODO here we can reuse old worker from pool of available now
                        try:
                            idle_task.set_status('active')
                            new_worker.execute()
                        except:
                            new_worker.set_status('error')
                            new_worker.set_status('error')
                            Exception("WORKER ERROR  worker type:[{}] id:[{}] creation failed"
                                      .format(idle_task.config.task_type , self.last_worker_created_ID - 1))





                #
                #
                #
                # for task_type in self.tasks:
                #
                #     # print("cheking {}".format(task_type))
                #
                #     available_data = len (self.data_manager.available_data_monitor[task_type])
                #
                #
                #
                #     #TODO check if it is lock safe
                #     if (available_data > 0):
                #         print('available [{}] data chunks for [{}] task'.format(available_data, task_type))
                #
                #
                #         src_file = [self.data_manager.available_data_monitor[task_type][0]]
                #
                #         self.data_manager.available_data_monitor[task_type].pop()
                #
                #
                #
                #         new_task_config = self.build_task_config(
                #             type=task_type,
                #             id=self.last_worker_created_ID,
                #             input_config={
                #                     "files":src_file,
                #                     "partitions" : [(1,1)]
                #                 },
                #         )
                #
                #         self.last_task_created_ID +=1
                #         self.spawn_task_from_config(new_task_config)
                #
                #
                #
                #     for task in self.tasks[task_type]:
                #         if task.is_idle():
                #             new_worker = self.spawn_worker(task) #TODO here we can reuse old worker from pool of available now
                #             try:
                #                 task.set_status('active')
                #                 new_worker.execute()
                #             except :
                #                 new_worker.set_status('error')
                #                 new_worker.status = 'error'
                #                 Exception("WORKER ERROR  worker type:[{}] id:[{}] creation failed"
                #                           .format(task_type, self.last_worker_created_ID-1))






    # def send_task(self):


    def spawn_worker(self, task, args=None):
        new_worker = Worker("worker_{}".format( self.last_worker_created_ID ), self.data_manager , self.pipeline_dict)
        self.last_worker_created_ID+=1
        try:
            new_worker.set_task(task)
            self.workers[task.config.task_type].append(new_worker)
            return new_worker

        except Exception: #todo looks like it misses some cases
            new_worker.set_status('error')
            print("spawning worker error")



    def print_help(self):
        pass


    def ping_worker(self):
        pass

