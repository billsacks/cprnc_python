"""Utilities for printing cprnc output"""

# ------------------------------------------------------------------------
# Public functions
# ------------------------------------------------------------------------

def index_str(dims):
    """Convert a list of indices into a pretty string for printing."""

    if (len(dims) == 0):
        return ""
    else:
        return "(" + ",".join(map(_format_index, dims)) + ")"


# ------------------------------------------------------------------------
# Private functions
# ------------------------------------------------------------------------

def _format_index(index):
    """Return a string version of the given array index"""

    return "{:6d}".format(index)
