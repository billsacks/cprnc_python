# Wrapper for scipy.io.netcdf, providing a common interface

from cprnc_py.netcdf.scipy.io.netcdf import netcdf_file as scipy_netcdf_file
from cprnc_py.netcdf.netcdf_file import NetcdfFile
from cprnc_py.netcdf.netcdf_variable_scipy import NetcdfVariableScipy
from cprnc_py.netcdf.fs_utils import tmpfs_copy

import warnings
import os

class NetcdfFileScipy(NetcdfFile):
    def __init__(self, filename, mode='r'):
        super(NetcdfFileScipy, self).__init__()
        self._copy = tmpfs_copy(filename)
        if self._copy:
            self._file = scipy_netcdf_file(self._copy, mode)
        else:
            self._file = scipy_netcdf_file(filename, mode)
        self._filename = filename

    def __del__(self):
        if self._copy:
            try:
                os.remove(self._copy)
            except:
                warnings.warn("Could not remove copy " + self._copy,
                              category=RuntimeWarning)

    def get_varlist(self):
        """Returns a list of variables in the netcdf file"""
        return self._file.variables.keys()

    def get_filename(self):
        """Returns the file name corresponding to this netcdf file"""
        return self._filename

    def get_global_attributes(self):
        """Returns a dictionary of global attributes"""
        
        # I don't like accessing the private _attributes variable, but I don't
        # see any other way to do this
        return self._file._attributes

    def get_dimlist(self):
        """Returns a list of dimensions in the netcdf file"""
        return self._file.dimensions.keys()

    def get_dimsize(self, dimname):
        """Returns the size of the given dimension.

        If this dimension doesn't exist, returns 0.

        If querying the unlimited dimension, and no variables on the file have
        this dimension, returns 0.
        """

        try:
            dimsize = self._file.dimensions[dimname]
        except KeyError:
            dimsize = 0
        else:
            if dimsize is None:
                # Unlimited dimensions have dimsize None. So find a variable with
                # this dimension, and look at its size.
                dimsize = self._get_dimsize_from_variables(dimname)
        return dimsize

    def _get_variable(self, varname):
        """Returns a NetcdfVariable-like object for the given variable"""

        return NetcdfVariableScipy(self._file.variables[varname])

    def has_variable(self, varname):
        """Returns True if the Netcdf file has the requested variable, otherwise False"""
        return varname in self._file.variables
