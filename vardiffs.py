from __future__ import print_function

import numpy as np
import numpy.ma as ma

class VarDiffs:
    def __init__(self, var_control, var_test):
        self._compute_stats(var_control, var_test)
        
    def vars_differ(self):
        return self._vars_differ
    
    def _compute_stats(self, var_control, var_test):
        if (ma.allequal(var_control, var_test)):
            self._vars_differ = False
        else:
            self._vars_differ = True

        
            
    