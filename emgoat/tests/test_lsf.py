
import pytest
from emgoat.cluster import LSFCluster


def test_jobs_info():
    for job in LSFCluster().get_jobs_info():
        print(job)


def test_nodes_info():
    for node in LSFCluster().get_nodes_info():
        print(node)

