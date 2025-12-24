
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



