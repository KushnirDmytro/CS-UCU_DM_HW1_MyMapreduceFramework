# class manages over operating nodes, creates processes, checks it's activity etc.

import sys
import os
import json
from Task import Task
from Worker import Worker





class MapReduceManager:

    def read_config(self, config_filename):
        try:
            with open(config_filename) as cfg:
                self.config_dict =  json.loads(cfg.read())
        except FileExistsError:
            print("config file reading from {} problem".format(config_filename))


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
                        print ("worker failed")





    # def send_task(self):




    def print_help(self):
        pass


    def read_confing(self):
        #todo check if config is ok values
        #todo check if configured functions have needed interface before spawning workers
        #todo mode for csv.files ==> Data manager
        pass


    def spawn_worker(self, task, args=None):
        new_worker = Worker("Jimm")
        try:
            new_worker.set_task(task)
            self.workers[task.task_type].append(new_worker)
            return new_worker

        except:
            new_worker.set_status('error')
            print()
            #TODO




    def ping_worker(self):
        pass

