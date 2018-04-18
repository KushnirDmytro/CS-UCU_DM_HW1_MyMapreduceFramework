# Class responsible for computing task and date representation


task_config = {
#todo
}

class Task:

    supported_types = ["map", "reduce", "combine", "suffle"]
    proces_status = ["active", "error", "idle", "finished"]


    def __init__(self, task_type, name, executable_dir, input_file, output_file):
        self.name=name
        self.type = task_type
        self.executable=executable_dir
        self.read_from=input_file
        self.write_to=output_file
        self.status="idle"
        self.data = [] #todo

        #todo ceckers and exceptions!
        if (self.type not in self.supported_types):
            print (ValueError)

        print("hello from new %s task /%s/ " % (task_type , name))

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