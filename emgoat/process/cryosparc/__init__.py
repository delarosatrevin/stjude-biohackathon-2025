
import shlex
from cryosparc.tools import CommandClient

from emgoat.cluster import Cluster
from emgoat.util import get_dict_from_args

SUFFIX_JOB = "cryosparc_"

class Command:
    def __init__(self, cmd, module):
        self.original_command = cmd.strip()
        parts = shlex.split(self.original_command)
        self.program_name = "" # The job_type that is going to be added later
        self.command_dict = get_dict_from_args(parts)
        self.cli = self.connect_cli_cryosparc(self.command_dict['--master_hostname'],
                                              self.command_dict["--master_command_core_port"][0],
                                              module.LICENSE_ID)


    def get_job_requirements(self, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """

        job_info = self.extract_job_information()
        self.program_name = SUFFIX_JOB + job_info["job_type"]
        # base_program_name = self.program_name.replace(SUFFIX_JOB, '')
        rule_func_name = f"_rule_{self.program_name}"
        requirements = None
        rule_func = getattr(self, rule_func_name, None)

        if rule_func is not None:
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self.program_name} job requirements.")

        return requirements

    def connect_cli_cryosparc(self, host, port, license_id):
        """ Try to connect to cryoSPARC CommandClient. """
        try:
            cli = CommandClient(host=host, port=port, headers={"License-ID": license_id})
            return cli
        except Exception as e:
            print(f"[ERROR] Could not connect to cryoSPARC at {host}:{port} - {e}")
            return None

    def extract_job_information(self):
        job_info = self.cli.get_job(self.command_dict["--project"], self.command_dict["--job"])
        job_type = job_info['job_type']
        job_params = job_info['params_base']

        job_resources = job_info['resources_needed']
        job_summ_info = {
            "job_type": job_type,
            "job_params": job_params,
            "job_resources": job_resources
        }

        return job_summ_info

    def _rule_cryosparc_import_particles(self):
        return Cluster.JobRequirements(ncpus=1)

    def _rule_cryosparc_import_volumes(self):
        return Cluster.JobRequirements(ncpus=1)

    def _rule_cryosparc_class_2D(self):
        # num_classes = job_params['class2D_K']['value'] # Specific for 2D
        return Cluster.JobRequirements(ngpus=2)

    def _rule_nonuniform_refine_new(self):
        # num_classes = job_params['class2D_K']['value'] # Specific for 2D
        return Cluster.JobRequirements(ngpus=4)


    def _rule_cryosparc_autopick(self): # TODO: check name
        return Cluster.JobRequirements(ncpus=16)
