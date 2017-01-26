from __future__ import print_function

from Queue import Empty
from multiprocessing import Process, Queue

class WorkObject(object):
    """This class encapsulates a method to call and the arguments (the work to perform)"""

    def __init__(self, routine, work, name = None):
        self._routine = routine
        self._work = work
        self._name = name

    def __str__(self):
        fmt_str = 'Work: {}\n'
        return fmt_str.format(self._work)

    def __eq__(self, other):
        return self._routine == other._routine and self._work == other._work

    def __hash__(self):
        return self._routine, self._name

    @classmethod
    def vector_work(cls, routine, work_list, name = None):
        """Returns a list of work objects with the same function and their own piece of work"""
        return [cls(routine, work, name) for work in work_list]

    @classmethod
    def map_to_workobjects(cls, routine_to_work, name = None):
        """
        Returns a list of work objects with the function given as the key to the map,
        and the work as the value
        """
        return [cls(routine, work, name)
                for routine, work in routine_to_work.items()]

    def perform_work(self):
        if type(self._work) == dict:
            return self._routine(**self._work)
        else:
            return self._routine(*self._work)

    def name(self):
        return self._name

class Process_Manager(object):
    """This class implements a producer-consumer multiprocessing scheme"""

    def __init__(self, nprocs=1):
        """Creates a Process_Manager object

        Arguments:
        worklist: A list of WorkObjects with work to be done
        nprocs: The number of processes to use"""
        self._work_queue = Queue()
        self._result_queue = Queue()
        self._result_list = None
        # Ensure that the child processes have this as False
        self._is_manager_proc = False

        # Ideally we'd use nprocs-1 processes and have the manager also perform work,
        # but that complicates things tremendously, so we don't
        # Note that this DOES incur significant performance cost when only one process is desired,
        # so we avoid this penalty by performing the work in the main process if there's only one
        if nprocs > 1:
            self._procs = [Process(target = self._do_work) for i in range(nprocs)]
        else:
            self._procs = []

        for p in self._procs:
            p.start()

        # The parent process should have this as True
        self._is_manager_proc = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._is_manager_proc == True:
            for p in self._procs:
                p.terminate()

    def work(self, work_list):
        if len(self._procs) > 0:
            for work in work_list:
                assert type(work) == WorkObject
                self._work_queue.put(work)
            for i in range(len(work_list)):
                result = self._result_queue.get(True)
                yield result
        else:
            for work in work_list:
                yield work.perform_work()

    def _do_work(self):
        while True:
            work = self._work_queue.get()
            result = work.perform_work()
            self._result_queue.put(result)
