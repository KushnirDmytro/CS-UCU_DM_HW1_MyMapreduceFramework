# Responsible for working with task, performing reduce or map itself, reads datafile writes output to
# other textfiles.


class Worker:



    def __init__(self):
        self.name="Undefined"
        self.data = []
        pass

    def read_from(self, filename):
        """
        As we presume immutability of reading file, no need to prevent datarace
        :param filename: name of file with inputs
        :return: string of file content
        """
        pass

    def write_res_to(self, filename):
        pass

    def set_task(self, callable_task):
        pass

    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        pass


