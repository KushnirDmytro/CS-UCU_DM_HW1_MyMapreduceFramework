# Class responsible for computing task and date representation


class Task:

    supported_types = ["map", "reduce", "combine", "suffle"]
    proces_status = ["active", "error", "idle", "finished"]

    def __init__(self, type, name, executable_dir, input_file, output_file):
        self.name=name
        self.type = type
        self.executable=executable_dir
        self.read_from=input_file
        self.write_to=output_file
        self.status="idle"
        self.data = []

    def get_executable_function(self, type, executable_dir):
        #todo check if file has appropriate interface function
        pass

