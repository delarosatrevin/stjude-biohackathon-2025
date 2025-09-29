
import shlex


class Command:
    def __init__(self, cmd):
        self._cmd = cmd
        self._cmd_dict = shlex.split(self._cmd)

    def get_job_requirements(self, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """
        rule_func_name = f"_rule_{self._program}"
        requirements = None
        if rule_func := getattr(self, rule_func_name):
            requirements = rule_func()

        if requirements is None:
            raise Exception(f"No rule found for {self._program} job requirements.")

        return requirements

    def relion_refine(self):
        pass

    def relion_import_particles(self):
        pass
