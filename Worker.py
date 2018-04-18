# Responsible for working with task, performing reduce or map itself, reads datafile writes output to
# other textfiles.

from Task import Task
from subprocess import Popen, PIPE
import sys

class Worker:

    worker_status = ['active', 'waiting_resource', 'idle', 'error', 'dead' ]

    def __init__(self, name):
        self.name=name
        self.task = None
        self.status = "idle"
        self.data = []
        self.process = None
        print ("Hello from worker {} status {} task {}".format(self.name, self.status, self.task) )



    def read_from(self, filename, diapasone = None):
        """
        As we presume immutability of reading file, no need to prevent datarace
        :param filename: name of file with inputs
        :return: string of file content
        """
        #todo add .csv
        pass


    def is_idle (self):
        return self.status == 'idle'


    def set_status(self, status):
        if status in self.worker_status:
            self.status = status
        else:
            pass
            #todo ValueError



    def write_res_to(self, filename):
        pass

    def set_task(self, task):
        if self.is_idle:
            self.task = task

    def execute(self):
        """
        designed for possibility of configurable evaluating flow
        :return:
        """
        try:
            self.set_status('active')
            self.task.status = 'active'
            process = Popen(
                [sys.executable,
                 "-u",
                 self.task.executable,
                 self.task.read_from,
                 self.task.write_to.format(self.task.name)],
                # stdout=PIPE,
                bufsize=1)
            process.communicate()
            # todo manage notification when and how process ended

            status = process.returncode
            #todo manage return code and administrate worker's status


            #todo Maybe should read and write file in this, not forked thread?

        except:
            #todo test if works
            ChildProcessError("failed to launch task {}__{}{} from worker {} ".format(self.task.executable,
                                                                                      self.task.name,
                                                                                      self.task.type,
                                                                                      self.name)  )




cmd = "example_word_counter_mapper.py data.txt worker_{}_"


#
# start = time.perf_counter()
#
#
#
# mappers=[]
# for i in range(int (config['mappers_n']) ):
#     cmd.format(i)
#     mappers.append(Popen([sys.executable, "-u", "example_word_counter_mapper.py", "data.txt", "map_worker_{}_.txt".format(i)], stdout=PIPE, bufsize=1))

