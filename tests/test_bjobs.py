import pytest
from lsf.bjobs import run_bjobs_get_alljobs, parse_bjobs_output_for_alljobs

def test_job_infor():

    output = run_bjobs_get_alljobs()
    job_list = parse_bjobs_output_for_alljobs(output)
    for job in job_list:
        print(job)

