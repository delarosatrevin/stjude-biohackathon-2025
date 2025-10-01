
PROCESS = {
    "module": "emgoat.process.cryosparc"
}

CLUSTER = {
    "module": "emgoat.cluster.lsf",
    "config": {}
}

MEM = "{{ ram_gb|int }}G"

GPUS = "{{ num_gpu }}"

LICENSE_ID = "9a8c7288-00b0-11ef-864f-ab66d8f93244"

COMMAND = "{{ run_cmd }}"

TEMPLATE = """
#!/usr/bin/env bash

#SBATCH --job-name cryosparc_{{ project_uid }}_{{ job_uid }}
#SBATCH --cpus-per-task={{ num_cpu }}
#SBATCH --gres=gpu:{{ num_gpu }}
#SBATCH --partition=standard
#SBATCH --mem={{ ram_gb|int }}G
#SBATCH --comment="created by {{ cryosparc_username }}"
#SBATCH --output={{ job_dir_abs }}/{{ project_uid }}_{{ job_uid }}_slurm.out
#SBATCH --error={{ job_dir_abs }}/{{ project_uid }}_{{ job_uid }}_slurm.err

{{ run_cmd }}
"""