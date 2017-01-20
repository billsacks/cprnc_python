from __future__ import print_function

import numpy as np
import numpy.ma as ma
from cprnc_py.print_utils import index_str

class VarInfo(object):
    """This class computes and prints a variety of statistics about a single
    variable.

    Typical usage is:

    (1) Create a VarInfo object:
        myinfo = VarInfo(var)

    (2) Print statistics:
        str(myinfo)
    """

    # ------------------------------------------------------------------------
    # Constructor and other special methods
    # ------------------------------------------------------------------------

    def __init__(self, var, name=None):
        """Create a VarInfo object.

        Arguments:
        var: numpy or numpy.ma array"""

        # Compute all necessary statistics in initialization, so that we don't
        # have to hold onto the variable in memory for later use (in case the
        # variable consumes a lot of memory).
        if not ma.isMA(var):
            var = ma.array(var)
        self._compute_stats(var)
        self.name = name

    def __str__(self):
        mystr = ""
        if self.name:
            mystr += self.name + "\n"
        mystr += "{:8d} {:8d} ".format(self._num_elements, self._num_valid)
        mystr += "{:23.15e} {} ".format(self._max_val, index_str(self._max_indices))
        mystr += "{:23.15e} {} ".format(self._min_val, index_str(self._min_indices))
        mystr += "{:23.15e}\n".format(self._mean_absval)

        return mystr

    # ------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------

    def _compute_stats(self, var):
        """Compute and store various statistics for later printing.

        Arguments:
        var: numpy.ma array"""

        # For performance reasons, it's better to use varc rather than var for
        # any operation that doesn't need to reference the array size or indices
        varc = var.compressed()

        self._shape = var.shape
        self._num_elements = var.size
        self._num_valid = varc.size
        if (self._num_valid > 0):
            self._max_val = varc.max()
            self._max_loc = var.argmax()
            self._min_val = varc.min()
            self._min_loc = var.argmin()
            self._mean_absval = (np.fabs(varc)).mean()
        else:
            self._max_val = 0
            self._max_loc = 0
            self._min_val = 0
            self._min_loc = 0
            self._mean_absval = 0
        if (var.ndim > 0 and var.shape[0] > 0):
            self._max_indices = np.unravel_index(self._max_loc, var.shape)
            self._min_indices = np.unravel_index(self._min_loc, var.shape)
        else:
            self._max_indices = 0
            self._min_indices = 0

class VarInfoNonNumeric(object):
    """This class represents a non-numeric variable"""

    def __init__(self, name):
        """Creates a VarInfoNonNumeric object"""
        self.name = name

    def __str__(self):
        return self.name + " is non-numeric and cannot be analyzed"
