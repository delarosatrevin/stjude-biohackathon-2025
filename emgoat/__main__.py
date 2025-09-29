#!/bin/python

import os
import sys
import argparse

import emgoat


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='emgoat')

    p.add_argument("template")
    p.add_argument('--debug', '-d', action="store_true",
                   help="Debug mode: just generate the template script"
                        "and print launching command. ")

    args = p.parse_args()

    emgoat.EMGoat(args.template, debug=args.debug).launch_job()
