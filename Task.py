# Class responsible for computing task and date representation


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
        self.read_from = task_config_dict ['input_file']
        self.write_to = task_config_dict ['output_file']
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
            pass
            #todo ValueError

    def get_executable_function(self, type, executable_dir):
        #todo check if file has appropriate interface function
        pass