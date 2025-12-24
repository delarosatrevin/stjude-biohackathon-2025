
import pytest
import os
import tempfile

from emgoat.cluster.lsf import Cluster as LSFCluster
from emgoat.cluster import Cluster


def test_jobs_info():
    for job in LSFCluster().get_jobs_info():
        print(job)
        print("\n\n")


def test_nodes_info():
    for node in LSFCluster().get_nodes_info():
        print(node)
        print("\n\n")

def test_accounts_info():
    for account in LSFCluster().get_accounts_info():
        print(account)
        print("\n\n")

def test_overview():
    lsf = LSFCluster()
    overview = lsf.get_cluster_overview(lsf.nodes_list)
    for key, value in overview.items():
        print("for the request gpu card: {0}, the available slot is {1} "
              "and percentage is {2}".format(key, value[0], value[1]))

    print("total number of gpu cards: {}".format(lsf.get_total_gpu_num(lsf.nodes_list)))
    print("total number of unused gpu cards: {}".format(lsf.get_total_unused_gpu_num(lsf.nodes_list)))



