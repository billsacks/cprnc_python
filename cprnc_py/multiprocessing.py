from __future__ import print_function

from multiprocessing import Process, Queue

class WorkObject(object):
    """This class encapsulates a method to call and the arguments (the work to perform)"""

    def __init__(self, routine, work, name = None):
        self._routine = routine
        self._work = work
        self._name = name

    def __str__(self):
        return self._name

    def __eq__(self, other):
        return self._routine == other._routine and self._work == other._work

    def __hash__(self):
        return self._routine, self._name

    @classmethod
    def single_routine(cls, routine, work_list, name = None):
        """Returns a list of work objects with the same function and their own piece of work"""
        return [cls(routine, work, name) for work in work_list]

    @classmethod
    def map_to_workobjects(cls, routine_to_work, name = None):
        return [cls(routine, work, name)
                for routine, work in routine_to_work.items()]

    def perform_work(self):
        if type(self._work) == dict:
            return self._routine(**self._work)
        else:
            return self._routine(*self._work)

    def name(self):
        return self._name

class Multiprocess(object):
    """This class implements a producer-consumer multiprocessing scheme"""

    def __init__(self, work_list, nprocs=1):
        """Creates a Multiprocess object

        Arguments:
        worklist: A list of WorkObjects with work to be done
        nprocs: The number of processes to use"""
        if len(work_list) < nprocs:
            nprocs = len(worklist)
        self._procs = [Process(target = self._work_queue) for i in range(nprocs - 1)]
        self._work_queue = Queue()
        for work in work_list:
            assert type(work) == WorkObject
            self._work_queue.put(work)
        self._result_queue = Queue()
        for p in self._procs:
            p.start()
        self._work_queue()

    def _work_queue(self):
        while True:
            try:
                work = self._work_queue.get(False)
            except Empty:
                break
            result = work.perform_work()
            self._result_queue.put(result)
