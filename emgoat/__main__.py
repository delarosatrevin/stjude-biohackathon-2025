#!/bin/python

import os
import sys
import argparse

import emgoat
from emgoat.cluster.lsf import Cluster as LSFCluster

here = os.path.abspath(os.path.dirname(__file__))


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='emgoat')

    # parameters for the action 
    p.add_argument('--generate_cluster_usage_data', action='store_true',
                   help='Generating json format of cluster usage data')

    # form the args
    args = p.parse_args()

    # for LSF cluster whether we generate the cluster usage data?
    if args.generate_cluster_usage_data:
        LSFCluster().generate_json_results()

    """
    Fenglai: currently disable the code below
    p.add_argument("template")
    p.add_argument('--debug', '-d', action="store_true",
                   help="Debug mode: just generate the template script"
                        "and print launching command. ")


    emgoat.EMGoat(args.template, debug=args.debug).launch_job()
    """
