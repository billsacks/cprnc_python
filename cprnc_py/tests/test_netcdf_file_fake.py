#!/usr/bin/env python

from __future__ import print_function

import unittest
from cprnc_py.netcdf.netcdf_file_fake import NetcdfFileFake
from cprnc_py.netcdf.netcdf_variable_fake import NetcdfVariableFake
import numpy as np
from cprnc_py.test_utils.custom_assertions import CustomAssertions

class TestNetcdfFileFake(CustomAssertions):
    def test_get_dimlist(self):
        var1 = NetcdfVariableFake(np.array([[1,2,3],[4,5,6]]),
                                  dimnames=['dim1','dim2'])
        var2 = NetcdfVariableFake(np.array([[1,2,3],[4,5,6]]),
                                  dimnames=['dim1','dim3'])
        fl = NetcdfFileFake('myfile', variables = {'var1':var1, 'var2':var2})
        mydims = sorted(fl.get_dimlist())
        self.assertEqual(['dim1','dim2','dim3'], mydims)

if __name__ == '__main__':
    unittest.main()
