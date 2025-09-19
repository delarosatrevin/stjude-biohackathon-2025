#!/bin/python

import os
import sys
import importlib

from emtools.utils import Pretty

jobfile = sys.argv[1]

Pretty.dprint(jobfile)

if not os.path.exists(jobfile):
    raise Exception("Missing file: " + jobfile)

spec = importlib.util.spec_from_file_location("jobfile", jobfile)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

print(dir(module))

