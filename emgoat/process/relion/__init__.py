
import shlex

from emgoat.cluster import Cluster
from emgoat.util import get_dict_from_args


class Command:
    def __init__(self, cmd):
        self.original_command = cmd.strip()
        parts = shlex.split(self.original_command)
        self.program_name = parts[1].replace('`', '')
        self.command_dict = get_dict_from_args(parts)

    def get_job_requirements(self, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """
        base_program_name = self.program_name.replace('_mpi', '')
        rule_func_name = f"_rule_{base_program_name}"
        requirements = None
        rule_func = getattr(self, rule_func_name, None)

        if rule_func is not None:
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self.program_name} job requirements.")

        return requirements

    def _rule_relion_refine(self):
        return Cluster.JobRequirements(ngpus=4)

    def _rule_relion_autopick(self):
        return Cluster.JobRequirements(ncpus=16)

    def _rule_relion_import_particles(self):
        return Cluster.JobRequirements(ncpus=1)
