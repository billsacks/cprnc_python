
import os
import shutil
import subprocess
import random
import string

"""
OS Dependent utility
Used to copy and remove files from a fast tmpfs filesystem
Does not work when the tmpfs directory has spaces due to mtab
"""

def find_tmpfs():
    """
    Finds all currently available tmpfs file systems from mtab
    If mtab does not exist or no tmpfs is mounted, return an empty list
    """
    mtab_path = os.path.join('/', 'etc', 'mtab')
    tmpfs = []
    if os.path.isfile(mtab_path):
        with open(mtab_path, 'r') as mtab:
            for l in mtab:
                if 'tmpfs' in l:
                    start = l.find('/')
                    end = l.rfind(' tmpfs')
                    path = l[start:end]
                    if os.path.isabs(path) and os.path.isdir(path):
                        tmpfs.append(path)
    return tmpfs

def get_tmpfname():
    """
    Creates a random filename for the copy,
    then returns the absolute path for the resulting copy
    """
    tmpfs_opts = find_tmpfs()
    if len(tmpfs_opts) >= 1:
        fname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        return os.path.join(tmpfs_opts[0], fname)
    else:
        return None

def tmpfs_copy_py(fpath):
    """
    Uses Python's shutil to copy the file.
    This is slightly slower than using the subprocess module to use the copy utility
    If no tmpfs exists, or if the copy fails, does nothing and returns None
    Otherwise, the resulting filename is returned
    """
    tmpfname = get_tmpfname()
    if tmpfname:
        try:
            shutil.copyfile(fpath, tmpfname)
        except IOError:
            pass
        else:
            return tmpfname
    return None

def tmpfs_copy_sh(fpath):
    """
    Uses the copy utility through the subprocess module
    This is slightly less nice than using shutil, and more OS dependent
    If no tmpfs exists, or if the copy fails, does nothing and returns None
    Otherwise, the resulting filename is returned
    """
    tmpfname = get_tmpfname()
    if tmpfname:
        cpcmd = ['cp', fpath, tmpfname]
        p = subprocess.Popen(cpcmd)
        p.wait()
        if p.returncode == 0:
            # Successful copy
            return tmpfname
    # Could not copy for one reason or another
    return None

tmpfs_copy = tmpfs_copy_sh
