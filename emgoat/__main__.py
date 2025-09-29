#!/bin/python

import os
import sys
import argparse

from emtools.utils import Pretty
from emgoat.util import Loader


def main():
    p = argparse.ArgumentParser(prog='emgoat')

    p.add_argument("template")
    p.add_argument('--debug', '-d', action="store_true",
                   help="Debug mode: just generate the template script"
                        "and print launching command. ")

    args = p.parse_args()

    template_file = args.template
    Pretty.dprint(template_file)
    template = Loader.load_from_file(template_file)
    print(dir(template))

    module_str = template.PROCESS['module']
    module = Loader.load_from_string(module_str)
    print(dir(module))




if __name__ == '__main__':
    main()
