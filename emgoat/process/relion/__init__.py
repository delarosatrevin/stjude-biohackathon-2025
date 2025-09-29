
import shlex

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
        rule_func_name = f"_rule_{self.program_name}"
        requirements = None
        if rule_func := getattr(self, rule_func_name):
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self.program_name} job requirements.")

        return requirements

    def relion_refine(self):
        pass

    def relion_import_particles(self):
        pass
