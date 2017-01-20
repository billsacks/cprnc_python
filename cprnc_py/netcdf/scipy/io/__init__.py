# The following allows modules within here to access other scipy snapshot files
# using the same import path as in the true scipy.
#
# For example, it allows us to keep the following in netcdf.py:
# from scipy._lib.six import integer_types, text_type, binary_type
# import cprnc_py.netcdf.scipy as scipy
from cprnc_py.netcdf import scipy
import sys
sys.modules['scipy'] = scipy
