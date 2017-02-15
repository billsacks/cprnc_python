#!/usr/bin/env python

from __future__ import print_function

import unittest
from cprnc_py.filediffs import FileDiffs
from cprnc_py.netcdf.netcdf_file_fake import NetcdfFileFake
from cprnc_py.netcdf.netcdf_variable_fake import NetcdfVariableFake
import numpy as np
from cprnc_py.test_utils.custom_assertions import CustomAssertions

class TestFilediffs(CustomAssertions):

    FILENAME1 = 'foo1.nc'
    FILENAME2 = 'foo2.nc'

    # ------------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------------

    def create_filediffs_with0Vars(self):
        """Create a filediffs object where both files have 0 vars"""
        file1 = NetcdfFileFake(self.FILENAME1)
        file2 = NetcdfFileFake(self.FILENAME2)
        return FileDiffs(file1, file2, separate_dim=None)
        

    # ------------------------------------------------------------------------
    # Tests of num_vars_differ
    # ------------------------------------------------------------------------

    def test_numVarsDiffer_with0Vars(self):
        mydiffs = self.create_filediffs_with0Vars()
        num_differ = mydiffs.num_vars_differ()
        self.assertEqual(num_differ, 0)

    def test_numVarsDiffer_with3Vars(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var2': NetcdfVariableFake(np.array([4,5,6])),
                         'var3': NetcdfVariableFake(np.array([7,8,9]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([99,2,3])),  # differs
                         'var2': NetcdfVariableFake(np.array([4,5,6])),   # same
                         'var3': NetcdfVariableFake(np.array([99,8,9]))}) # differs
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        num_differ = mydiffs.num_vars_differ()
        self.assertEqual(num_differ, 2)

    def test_numVarsDiffer_withVariableSeparatedByDim(self):
        # make sure that filediffs separates a variable by some dimension if
        # requested to do so
        data = np.array([[1,2,3],[4,5,6]])
        data_plus_1 = data + 1
        variable = NetcdfVariableFake(data, ('dim1', 'dim2'))
        variable_plus_1 = NetcdfVariableFake(data_plus_1, ('dim1', 'dim2'))
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': variable})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': variable_plus_1})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_differ = mydiffs.num_vars_differ()
        self.assertEqual(num_differ, 3)


    def test_numVarsDiffer_withVariableWithoutSeparateDim(self):
        # If we ask to separate on a given dimension, but a given variable
        # doesn't have that dimension, make sure this variable is just counted
        # once
        data = np.array([[1,2,3],[4,5,6]])
        data_plus_1 = data + 1
        variable = NetcdfVariableFake(data, ('dim1', 'dim2'))
        variable_plus_1 = NetcdfVariableFake(data_plus_1, ('dim1', 'dim2'))
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': variable})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': variable_plus_1})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim_does_not_exist')
        num_differ = mydiffs.num_vars_differ()
        self.assertEqual(num_differ, 1)

    # ------------------------------------------------------------------------
    # Tests of num_masks_differ
    # ------------------------------------------------------------------------

    def test_numMasksDiffer_with3Vars(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.ma.array([1,2,3], mask=[True,False,False])),
                         'var2': NetcdfVariableFake(np.ma.array([4,5,6], mask=[False,True,False])),
                         'var3': NetcdfVariableFake(np.ma.array([7,8,9], mask=[False,False,True]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.ma.array([1,2,3], mask=[False,True,False])),  # differs
                         'var2': NetcdfVariableFake(np.ma.array([4,5,6], mask=[False,True,False])),  # same
                         'var3': NetcdfVariableFake(np.ma.array([7,8,9], mask=[False,True,False]))}) # differs
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        num_differ = mydiffs.num_masks_differ()
        self.assertEqual(num_differ, 2)

    # ------------------------------------------------------------------------
    # Tests of num_dims_differ
    # ------------------------------------------------------------------------

    def test_numDimsDiffer_with3Vars(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var2': NetcdfVariableFake(np.array([4,5,6])),
                         'var3': NetcdfVariableFake(np.array([7,8,9]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2])),       # differs
                         'var2': NetcdfVariableFake(np.array([4,5,6])),     # same
                         'var3': NetcdfVariableFake(np.array([7,8,9,10]))}) # differs
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        num_differ = mydiffs.num_dims_differ()
        self.assertEqual(num_differ, 2)

    # ------------------------------------------------------------------------
    # Tests of num_could_not_be_analyzed
    # ------------------------------------------------------------------------

    def test_numCouldNotBeAnalyzed_withCharVariable(self):
        var_numeric = NetcdfVariableFake(np.array([1,2,3]))
        var_char = NetcdfVariableFake(np.array(['a','b','c']), is_numeric=False)
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': var_numeric,
                         'var2': var_char})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': var_numeric,
                         'var2': var_char})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        num_could_not_be_analyzed = mydiffs.num_could_not_be_analyzed()
        self.assertEqual(num_could_not_be_analyzed, 1)

    # ------------------------------------------------------------------------
    # Tests of num_nonshared_fields
    # ------------------------------------------------------------------------

    def test_numNonsharedFields_noNonsharedFields(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var2': NetcdfVariableFake(np.array([4,5,6]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var2': NetcdfVariableFake(np.array([4,5,6]))})
        mydiffs = FileDiffs(file1, file1, separate_dim=None)
        num_nonshared = mydiffs.num_nonshared_fields()
        self.assertEqual(num_nonshared, 0)

    def test_numNonsharedFields_withNonsharedFields(self):
        # Both files have var1; file1 has var2, whereas file2 has var3
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var2': NetcdfVariableFake(np.array([4,5,6]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3])),
                         'var3': NetcdfVariableFake(np.array([4,5,6]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        num_nonshared = mydiffs.num_nonshared_fields()
        # Total number of nonshared fields is equal to (# in file1 not in file2)
        # + (# in file2 not in file1)
        self.assertEqual(num_nonshared, 2)

    def test_numNonsharedFields_separateDim_noNonsharedFields(self):
        data = np.array([[1,2,3],[4,5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var2': NetcdfVariableFake(data, ('dim3', 'dim2'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var2': NetcdfVariableFake(data, ('dim3', 'dim2'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_nonshared = mydiffs.num_nonshared_fields()
        self.assertEqual(num_nonshared, 0)

    def test_numNonsharedFields_separateDim_withNonsharedFields(self):
        # Both files have var1; file1 has var2, whereas file2 has var3
        data = np.array([[1,2,3],[4,5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var2': NetcdfVariableFake(data, ('dim3', 'dim2'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var3': NetcdfVariableFake(data, ('dim3', 'dim2'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_nonshared = mydiffs.num_nonshared_fields()
        # Total number of nonshared fields is equal to (# in file1 not in file2)
        # + (# in file2 not in file1). Fields are counted once for each slice
        # along the separate_dim.
        self.assertEqual(num_nonshared, 6)

    def test_numNonsharedFields_separateDim_withNonsharedUnseparatedFields(self):
        # Both files have var1; file1 has var2, whereas file2 has var3; those
        # unshared fields do not have the separate_dim
        data = np.array([[1,2,3],[4,5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var2': NetcdfVariableFake(data, ('dim3', 'dim4'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2')),
                         'var3': NetcdfVariableFake(data, ('dim3', 'dim4'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_nonshared = mydiffs.num_nonshared_fields()
        # Total number of nonshared fields is equal to (# in file1 not in file2)
        # + (# in file2 not in file1). The nonshared fields don't have the
        # separate_dim, so each is counted once.
        self.assertEqual(num_nonshared, 2)

    def test_numNonsharedFields_separateDim_varHasDimInOne(self):
        # Doing diffs with a separate_dim, where a variable has the separate_dim
        # in one file but not in the other
        data = np.array([[1,2,3],[4,5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim2'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data, ('dim1', 'dim3'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_nonshared = mydiffs.num_nonshared_fields()
        # Total number of nonshared fields is equal to (# in file1 not in file2)
        # + (# in file2 not in file1). In file1, we effectively have 3
        # variables, since var1 is separated along dim2; none of these are
        # present in file2, since var1 there has no dim2. In file2 we have 1
        # variable (var1 with no separate_dim), which is not found in file1.
        self.assertEqual(num_nonshared, 4)

    def test_numNonsharedFields_separateDim_differentNumberOfSlices(self):
        # Doing diffs with a separate_dim, where variable has a different number
        # of slices in the two variables. There should be one nonshared field
        # for each missing slice. Note that we treat this as non-shared, rather
        # than a dim-diff; this feels like the right thing to do, both in terms
        # of:
        # - By analogy to the fact that we treat each time slice as a separate
        #   variable for the sake of analysis
        # - It seems like this is more likely to do the Right Thing in the case
        #   of intentional / expected differences in the number of time slices in
        #   the file
        data1 = np.array([[1,2,3,4],[5,6,7,8]])
        data2 = np.array([[1,2],[5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data1, ('dim1', 'dim2'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data2, ('dim1', 'dim2'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        num_nonshared = mydiffs.num_nonshared_fields()
        self.assertEqual(num_nonshared, 2)

        # Also make sure that other counts are correct in this case
        self.assertEqual(mydiffs.num_vars(), 4)
        self.assertEqual(mydiffs.num_vars_differ(), 0)
        self.assertEqual(mydiffs.num_dims_differ(), 0)

    # ------------------------------------------------------------------------
    # Tests of files_differ
    # ------------------------------------------------------------------------

    def test_filesDiffer_withEqualVariables(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        differ = mydiffs.files_differ()
        self.assertFalse(differ)

    def test_filesDiffer_withVarsDiffer(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,4]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withMasksDiffer(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.ma.array([1,2,3], mask=[True,False,False]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.ma.array([1,2,3], mask=[False,True,False]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withDimsDiffer(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withNoVariables(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {})
        mydiffs = FileDiffs(file1, file2)
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withNoVarsAnalyzed(self):
        var_char = NetcdfVariableFake(np.array(['a','b','c']), is_numeric=False)
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': var_char})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': var_char})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withDifferentSlices(self):
        # Files with variables with different slices should also differ
        data1 = np.array([[1,2,3,4],[5,6,7,8]])
        data2 = np.array([[1,2],[5,6]])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(data1, ('dim1', 'dim2'))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(data2, ('dim1', 'dim2'))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    def test_filesDiffer_withDifferentVars(self):
        # Files with different variables should differ
        data = np.array([1,2,3,4])
        file1 = NetcdfFileFake(
            self.FILENAME1,
            # Tuples with only one string must have a comma appended
            variables = {'var1': NetcdfVariableFake(data, ('dim',))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var4': NetcdfVariableFake(data, ('dim',))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim2')
        differ = mydiffs.files_differ()
        self.assertTrue(differ)

    # ------------------------------------------------------------------------
    # Tests of __str__
    #
    # Currently we just test the critical piece: the final statement of whether
    # the files are DIFFERENT or IDENTICAL
    # ------------------------------------------------------------------------

    def test_str_differ(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,4]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        mystr = str(mydiffs)
        self.assertRegexMatches(mystr, "diff_test.*DIFFERENT")
        self.assertNotRegexMatches(mystr, "diff_test.*IDENTICAL")

    def test_str_identical(self):
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        mydiffs = FileDiffs(file1, file2, separate_dim=None)
        mystr = str(mydiffs)
        self.assertRegexMatches(mystr, "diff_test.*IDENTICAL")
        self.assertNotRegexMatches(mystr, "diff_test.*DIFFERENT")

    def test_str_separateDim(self):
        # Just a smoke test of the printing when separate_dim is given
        file1 = NetcdfFileFake(
            self.FILENAME1,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,3]))})
        file2 = NetcdfFileFake(
            self.FILENAME2,
            variables = {'var1': NetcdfVariableFake(np.array([1,2,4]))})
        mydiffs = FileDiffs(file1, file2, separate_dim='dim1')
        mystr = str(mydiffs)

if __name__ == '__main__':
    unittest.main()
