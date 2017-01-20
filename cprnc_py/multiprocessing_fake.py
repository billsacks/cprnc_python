"""Fake replacement for the multiprocessing package.

This version doesn't do any multiprocessing, but satisfies the necessary
interface. The point of this is so we can avoid the overhead of the true
multiprocessing package when just using one proc.
"""

class PoolFake(object):
    def __init__(self):
        self.map = map
