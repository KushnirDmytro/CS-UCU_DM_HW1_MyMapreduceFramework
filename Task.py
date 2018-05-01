# Class responsible for computing task and date representation

import importlib
task_config = {
#todo
}


class Task:

    supported_types = ["map",  "combine", "shuffle", "reduce"]
    proces_status = ["active", "waiting_resource", "error", "idle", "finished"]

    class Resources:
        def __init__(self, resources): #TODO refactor this and default (1,1) partitioning
            self.files = resources["files"]
            self.partitions = resources["partitions"]

    class TaskConfig:
        def __init__(self, task_config_dict):
            self.task_type = task_config_dict['task_type']
            self.ID = task_config_dict['ID']
            self.executable_dir = task_config_dict['executable_dir']
            self.input_src = Task.Resources(task_config_dict['input_src'])


    def __init__(self, task_config_dict):
        self.config = self.TaskConfig(task_config_dict)

        self.status = "idle" #task_config_dict ['']


        #todo ceckers and exceptions!
        if (self.config.task_type not in self.supported_types):
            print (ValueError)

        print("hello from new [%s] task ID:[%s] " % (self.config.task_type , self.config.ID))

    def is_idle(self):
        return self.status == 'idle'

    def set_status(self, status):
        if status in self.proces_status:
            self.status = status
        else:
            raise ValueError("No such value [{}] among possible states".format(status))


    def get_executable_function(self):

        executable_module = importlib.import_module(self.config.executable_dir)

        task_type = self.config.task_type

        if task_type == 'combine':
            callable_method_name = 'reduce'
        else:
            callable_method_name = self.config.task_type #they are similar strings


        if not hasattr(executable_module, callable_method_name):
            raise AttributeError("used executable {} has no appropriate method {} ".format(self.config.executable_dir,
                                                                                         callable_method_name))
        else:
            function_to_call = getattr(executable_module, callable_method_name)
            return function_to_call
