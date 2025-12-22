import pytest
from emgoat.cluster.lsf.lsf_jobs import *

def testing_lsf_jobs():
    """
    testing teh lsf job data generation
    """
    output = run_bjobs_get_alljobs()
    job_infor = parse_bjobs_output_for_alljobs(output)
    print("this is the raw output from bjobs command")
    for r in job_infor:
        print(r)

    # write the result and read it again
    generate_json_jobs_info()
    result = read_json_jobs_info()
    print("This is the output from reading bjobs data file, should be same with above results")
    for r in result:
        print(r)
