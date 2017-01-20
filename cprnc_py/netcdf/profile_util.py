
import numpy as np
import random
import math
import cProfile
import pstats
import os
import inspect

def get_class_from_frame(fr):
  import inspect
  args, _, _, value_dict = inspect.getargvalues(fr)
  # we check the first parameter for the frame function is
  # named 'self'
  if len(args) and args[0] == 'self':
    # in that case, 'self' will be referenced in value_dict
    instance = value_dict.get('self', None)
    if instance:
      # return its class
      return str(getattr(instance, '__class__', None))[8:-2] + '::'
  # return __GLOBAL__ otherwise
  return '__GLOBAL__::'

def init_fn_tracer():
  trace_callgraph = {}

  def trace(frame, event, arg):
    if 'call' in event or  'return' in event:
      fn = frame.f_code.co_name
      filename = frame.f_code.co_filename
      if 'ACME/cime' not in filename or 'expect' in fn:
        return
      parentclass = get_class_from_frame(frame.f_back)
      parentfilename = frame.f_back.f_code.co_filename
      parent = parentfilename + ' >> ' + parentclass + frame.f_back.f_code.co_name
      line = str(frame.f_back.f_lineno)
      classname = get_class_from_frame(frame)
      callee = line + ' ' * (4 - len(line)) + ' -> ' + classname + fn
      if parent not in trace_callgraph:
        fn_tracer.trace_callgraph[parent] = set([callee])
      else:
        fn_tracer.trace_callgraph[parent].add(callee)
  sys.settrace(trace)

  def genGraph():
    outstr = ''
    for caller in trace_callgraph:
      outstr += caller + ':\n'
      for callee in sorted(trace_callgraph[caller]):
        outstr += '  ' + callee + '\n'
        outstr += '\n'
    print(outstr)
  return genGraph

def nullInit():
  return {}

def randomHeight():
  height = 1
  while random.randint(1, 2) != 1:
    height += 1
  return height

def randomHeightNP():
  return -np.frexp(np.random.random())[1]

def randomHeightPy():
  return 1 - math.frexp(random.random())[1]

def histFunction(fn, number = 1000):
  hist = {}
  for i in range(number):
    k = fn()
    if k in hist:
      hist[k] += 1
    else:
      hist[k] = 1
  return hist

def initFN():
  start = random.randint(0, sz - 1)
  bytes = random.randint(1, sz - start - 1)
  return {"start": start, "sz": bytes}

def readDataF(start, sz):
  f.seek(start)
  f.read(sz)

def readDataBF(start, sz):
  bf.read(start, sz)

def _profFunctionNoParams(fn, number, prof, cleanfn = None):
  for i in range(number):
    prof.enable()
    fn()
    prof.disable()
    if cleanfn:
      cleanfn()
  return pstats.Stats(prof)

def _profFunctionParams(fn, initfn, number, prof, cleanfn = None):
  for i in range(number):
    args = initfn()
    prof.enable()
    fn(**args)
    prof.disable()
    if cleanfn:
      cleanfn(**args)
  return pstats.Stats(prof)
  
def profFunction(fn, initfn = None, cleanfn = None, number = 1000):
  prof = cProfile.Profile()
  if initfn == None:
    fn()
    if cleanfn:
      cleanfn()
    return _profFunctionNoParams(fn, number, prof, cleanfn)
  else:
    args = initfn()
    fn(**args)
    if cleanfn:
      cleanfn(**args)
    return _profFunctionParams(fn, initfn, number, prof, cleanfn)

import fs_utils

tmpfs = fs_utils.find_tmpfs()[0]
copyfn = '/gscratch/mdeakin/cprnc_python/copytest'

def copy_py():
  fs_utils.tmpfs_copy(copyfn, tmpfs)

def copy_sh():
  fs_utils.tmpfs_copy_sh(copyfn, tmpfs)

def copy_clean():
  fs_utils.rm_tmpfs_copy(copyfn, tmpfs)

class FixedVar:
  def __init__(self, i):
    self.i = i

class DynVar:
  def __init__(self, varName, varValue):
    exec("self." + varName + "=" + str(varValue))

class MapVar:
  def __init__(self, varName, varValue):
    self.state = {varName: varValue}

if __name__ == '__main__':

  profs = {
    'Copy Python': {'fn': copy_py, 'initfn': None, 'cleanfn': copy_clean, 'number': 10},
    'Copy Shell': {'fn': copy_sh, 'initfn': None, 'cleanfn': copy_clean, 'number': 10}
    }

  for pName in profs:
    print(pName)
    profFunction(**profs[pName]).print_stats()
    print()
