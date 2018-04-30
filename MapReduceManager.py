# class manages over operating nodes, creates processes, checks it's activity etc.

import sys
import os
import json
from Task import Task
from Worker import Worker





class MapReduceManager:
    """
    We don't need special storage for ready data, because they are output directory names for each finished task
    """

    def read_config(self, config_filename):
        # todo check if config is ok values
        # todo mode for csv.files ==> Data manager
        try:
            with open(config_filename) as cfg:
                self.config_dict =  json.loads(cfg.read())
        except FileExistsError:
            print("config file reading from {} problem".format(config_filename))


    def create_tasks_pipeline(self):
        pass


    def __init__(self, config_filename="config.json"):
        print("hello from MasteNode1")

        self.read_config(config_filename)

        self.task_types = Task.supported_types
        print ("Task types", self.task_types)

        self.workers = {}
        self.tasks = {}
        for task_type in self.task_types:
            self.workers[task_type] = [] #holders for workers and tasks
            self.tasks[task_type] = []


        self.help_msg = "" #<== TODO <maybe clean

        self.last_worker_created_ID = 0
        self.last_task_created_ID = 0




    def add_task(self, task_config):
        new_task = Task(task_config)

        self.tasks[new_task.task_type].append(new_task)



    def run(self):
        for task_type in self.tasks:
            print("cheking {}".format(task_type))
            for task in self.tasks[task_type]:
                if task.is_idle():
                    new_worker = self.spawn_worker(task)
                    try:
                        task.set_status('active')
                        new_worker.execute()
                    except Exception:
                        new_worker.set_status('error')
                        new_worker.status = 'error'
                        print ("worker failed") #TODO nice error msg logic





    # def send_task(self):




    def print_help(self):
        pass



    def spawn_worker(self, task, args=None):
        new_worker = Worker("Jimm")
        try:
            new_worker.set_task(task)
            self.workers[task.task_type].append(new_worker)
            return new_worker

        except Exception:
            new_worker.set_status('error')
            print()




    def ping_worker(self):
        pass

