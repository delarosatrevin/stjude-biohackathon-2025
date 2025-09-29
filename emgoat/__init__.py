
__version__ = "0.0.1"

from .config import config
from pprint import pprint


class EMGoat:

    def __init__(self, template_file, debug=False):
        from .util import Loader

        self.debug = debug
        template = Loader.load_from_file(template_file)
        module_str = template.PROCESS['module']
        process_module = Loader.load_from_string(module_str)
        self.cmd = process_module.Command(template.COMMAND)

    def launch_job(self):
        requirements = self.cmd.get_job_requirements()

        if self.debug:
            pprint(requirements)
