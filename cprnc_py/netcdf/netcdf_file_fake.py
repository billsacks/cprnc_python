from cprnc_py.netcdf.netcdf_file import NetcdfFile

class NetcdfFileFake(NetcdfFile):
    """Fake replacement for a NetcdfFile, for the sake of unit testing.

    This version allows unit tests to add whatever data they want before calling
    a method on the system under test."""
    # ------------------------------------------------------------------------
    # Test-specific interface
    # ------------------------------------------------------------------------
    def __init__(self, filename, variables={}, global_attributes={}):
        """Initialize a NetcdfFileFake instance.

        Arguments:

        filename: string giving the fake filename (this is not actually read, so
        can be a nonexistent file)

        variables: dictionary of variables, where the values are instances of
        NetcdfVariableFake

        global_attributes: dictionary of "global attributes" of the fake file
        """
        super(NetcdfFileFake, self).__init__()
        self._variables = variables
        self._filename = filename
        self._global_attributes = global_attributes

    def add_variable(self, varname, variable):
        """Add a variable to the variable dictionary.

        Arguments:
        varname: string
        variable: instance of NetcdfVariableFake
        """
        self._variables[varname] = variable

    # ------------------------------------------------------------------------
    # Replacements for real functionality
    # ------------------------------------------------------------------------
    def get_varlist(self):
        """Returns an iterable list of variables in the netcdf file"""
        return self._variables.keys()

    def get_filename(self):
        return self._filename

    def get_global_attributes(self):
        return self._global_attributes
    
    def get_dimsize(self, dimname):
        """Returns the size of the given dimension.

        Assumes that we can get the dimension from the variables on the file,
        which in turn assumes that the variables were defined with compatible
        dimensions (e.g., if var1 and var2 both have dimension dim1, then
        assumes that the size of dim1 agrees in the two variables).

        If no variables contain this dimension, returns 0.
        """
        return self._get_dimsize_from_variables(dimname)

    def get_dimlist(self):
        """Returns a list of dimensions in the netcdf file.

        Assumes that we can get the list of dimensions from the dimensions
        associated with each variables.
        """
        mydims = set()
        for var in self._variables:
            mydims = mydims | set(self._variables[var].get_dimensions())
        return list(mydims)

    def _get_variable(self, varname):
        """Returns a NetcdfVariable-like object for the given variable"""
        return self._variables[varname]

    def has_variable(self, varname):
        """Returns True if the Netcdf file has the requested variable, otherwise False"""
        return varname in self._variables
