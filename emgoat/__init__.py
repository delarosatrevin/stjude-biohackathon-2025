
__version__ = "0.0.1"

import os
from .config import config
from pprint import pprint


class EMGoat:

    def __init__(self, template_file, debug=False):
        from .util import Loader
        self.template_file = template_file
        self.debug = debug
        template_module = Loader.load_from_file(template_file)
        module_str = template_module.PROCESS['module']
        process_module = Loader.load_from_string(module_str)
        self.cmd = process_module.Command(template_module)

    def launch_job(self):
        requirements = self.cmd.get_job_requirements()

        if self.debug:
            print(requirements)
        else:
            # FIXME: This should be done using the Cluster class
            pass
