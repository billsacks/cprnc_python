class NetcdfFile(object):
    """Base class providing operations that can be performed on a netcdf file.

    This should not be instantiated directly. Instead, instantiate one of its
    subclasses, which provide support for particular python netcdf packages.

    This class serves two purposes:

    (1) Provides a common API through which any python netCDF package can be
    used; this includes:

    - get_varlist(): Returns a list of variables in the netcdf file

    - get_filename(): Returns the file name corresponding to this netcdf file

    - get_global_attributes(): Returns a dictionary of global attributes

    - get_dimsize(dimname): Returns the size of the given dimension

    (2) Provides some higher-level methods on top of these netCDF packages, such
    as getting variable data sliced by one or more named dimensions; this
    includes:

    - get_varlist_by_dim(dimname): Generator that yields a tuple (varname,
      index), with one return for each index in the given dimname

    - get_vardata(varname, dim_indices): Returns the variable's data, possibly
      sliced along one or more named dimensions

    - is_var_numeric(varname): Returns True if the given variable is numeric
    """

    # ------------------------------------------------------------------------
    # Public methods implemented here
    # ------------------------------------------------------------------------

    def get_varlist_bydim(self, dimname):
        """Generator that yields a tuple (varname, index).

        Each variable in the file is returned at least once. For variables that
        have a dimension given by dimname, there is a separate return for each
        index of that dimension. For variables that do not have the given
        dimension, the index will be None.

        For example, if a file contains:
        foo1(dim1, dim2)
        foo2(dim1)
        foo3(dim1, dim2)
        foo4(dim1)
        where dim2 is size 3

        and get_varlist_bydim is called with dimname = 'dim2', then it will
        yield:
        ('foo2', None)
        ('foo4', None)
        ('foo1', 0)
        ('foo3', 0)
        ('foo1', 1)
        ('foo3', 1)
        ('foo1', 2)
        ('foo3', 2)
        """

        dimsize = self.get_dimsize(dimname)

        # First yield variables that do not have the given dimension
        for varname in sorted(self.get_varlist()):
            var = self._get_variable(varname)
            dimnum = var.get_dimnum(dimname)
            if dimnum is None:
                yield (varname, None)

        # Now yield variables with the given dimension
        for index in range(dimsize):
            for varname in sorted(self.get_varlist()):
                var = self._get_variable(varname)
                dimnum = var.get_dimnum(dimname)
                if dimnum is not None:
                    yield (varname, index)
        
    def get_vardata(self, varname, dim_indices={}):
        """Get data corresponding to the given variable name, as a numpy (or
        numpy.ma) array.

        If dim_indices is given, it should be a dictionary whose keys are names of
        dimensions and whose values are the index to use for that dimension.

        For example, if we have a variable foo(lat,lon,time), and dim_indices =
        {'lat':3, 'time':0}, then get_vardata will return an array that contains
        foo[3,:,0].

        A dim_indices value of None means to ignore that entry, thus returning
        all slices for that dimension. e.g., if we have a variable
        foo(lat,lon,time), and dim_indices = {'lat':3, 'time':None}, then
        get_data will return an array that contains foo[3,:,:].
        """
        var = self._get_variable(varname)
        return var.get_data(dim_indices)

    def is_var_numeric(self, varname):
        """Returns True if the given variable is numeric, False otherwise (e.g.,
        if it is a character variable)."""

        var = self._get_variable(varname)
        return var.is_numeric()

    # ------------------------------------------------------------------------
    # Public methods that should be provided by subclasses
    # ------------------------------------------------------------------------

    def get_varlist(self):
        """Returns a list of variables in the netcdf file"""
        raise NotImplementedError

    def get_filename(self):
        """Returns the file name corresponding to this netcdf file"""
        raise NotImplementedError

    def get_global_attributes(self):
        """Returns a dictionary of global attributes"""
        raise NotImplementedError

    def get_dimsize(self, dimname):
        """Returns the size of the given dimension"""
        raise NotImplementedError

    def has_variable(self, varname):
        """Returns True if the file has the variable, otherwise False"""
        raise NotImplementedError

    # ------------------------------------------------------------------------
    # Private methods implemented here
    # ------------------------------------------------------------------------

    def _get_dimsize_from_variables(self, dimname):
        """Returns the size of the given dimension by inspecting variables that
        have this dimension.

        This is a utility method that can be used by NetcdfFile subclasses that
        cannot get dimension sizes directly. This is the case, for example, when
        the file's dimension information gives a size of None for unlimited
        dimensions.

        If no variables contain this dimension, returns 0.
        """

        for varname in self.get_varlist():
            var = self._get_variable(varname)
            dimnum = var.get_dimnum(dimname)
            if dimnum is not None:
                dimsize = var.get_shape()[dimnum]
                break
        else:
            dimsize = 0

        return dimsize

    # ------------------------------------------------------------------------
    # Private methods that should be provided by subclasses
    # ------------------------------------------------------------------------

    def _get_variable(self, varname):
        """Returns a NetcdfVariable-like object for the given variable"""
        raise NotImplementedError

