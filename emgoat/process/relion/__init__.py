
class Command:
    pass


class Rules:
    @classmethod
    def get_job_requirements(cls, command, cluster=None):
        """ Depending on the job name and inputs,
        determine the job requirements for execution. """
        pass

    @classmethod
    def relion_refine(cls, **kwargs):
        pass

    @classmethod
    def relion_import_particles(cls, **kwargs):
        pass