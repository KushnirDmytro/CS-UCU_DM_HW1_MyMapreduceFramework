# Class responsible for computing task and date representation

import importlib
task_config = {
#todo
}

class Task:

    supported_types = ["map", "reduce", "combine", "suffle"]
    proces_status = ["active", "error", "idle", "finished"]


    def __init__(self, task_config_dict):
        self.ID= task_config_dict ['ID']
        self.task_type = task_config_dict ['task_type']
        self.executable = task_config_dict ['executable_dir']
        self.read_from = task_config_dict ['input_files']
        self.write_to = task_config_dict ['output_files']
        self.status = "idle" #task_config_dict ['']


        #todo ceckers and exceptions!
        if (self.task_type not in self.supported_types):
            print (ValueError)

        print("hello from new %s task /%s/ " % (self.task_type , self.ID))

    def is_idle(self):
        return self.status == 'idle'

    def set_status(self, status):
        if status in self.proces_status:
            self.status = status
        else:
            raise ValueError("No such value [{}] among possible states".format(status))


    def get_executable_function(self):

        executable_module = importlib.import_module(self.executable)
        callable_method_name = self.task_type
        if not hasattr(executable_module, callable_method_name):
            raise AttributeError("used executable {} has no appropriate method {} ".format(self.executable,
                                                                                         callable_method_name))
        else:
            function_to_call = getattr(executable_module, callable_method_name)
            return function_to_call
