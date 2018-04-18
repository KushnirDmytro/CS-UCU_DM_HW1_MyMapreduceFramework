# class manages over operating nodes, creates processes, checks it's activity etc.

import sys
import os
import json
from Task import Task
from Worker import Worker



class MapReduceManeger:


    def __init__(self, arguments=None, config_filename="config.json"):
        print("hello from MasteNode1")

        try:
            with open(config_filename) as cfg:
                self.config =  json.loads(cfg.read())
        except FileExistsError:
            print("config file reading from {} problem".format(config_filename))

        # todo default values
        self.task_types= Task.supported_types
        print ("Task types", self.task_types)

        self.mapers_workers = {}
        self.mapper_tasks = {}

        for task_type in self.task_types:
            self.mapers_workers[task_type] = [] #holders for workers and tasks
            self.mapper_tasks[task_type] = []

        self.reducers_register = []
        self.workers_register = []

        self.help_msg = ""
        self.data = []


    def spawn_task(self, task_config):
        new_task = Task(type = task_config['type'],
                        name = task_config['name'],
                        executable_dir = task_config['executable_dir'],
                        input_file = task_config['input_file'],
                        output_file = task_config['output_file']
                        )
        #todo disputable class...
        pass

    def send_task(self):
        pass


    def print_help(self):
        pass


    def read_confing(self):
        #todo check if config is ok values
        # todo check if configured functions have needed interface before spawning workers
        #todo mode for csv.files
        pass


    def spawn_worker(self, task, args):
        pass


    def ping_worker(self):
        pass

