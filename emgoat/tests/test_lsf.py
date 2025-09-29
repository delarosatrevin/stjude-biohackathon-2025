
import pytest
from emgoat.cluster import LSFCluster


def test_jobs_info():
    for job in LSFCluster().get_jobs_info():
        print(job)


def test_nodes_info():
    for node in LSFCluster().get_nodes_info():
        print(node)

def test_accounts_info():
    for account in LSFCluster().get_accounts_info():
        print(account)

