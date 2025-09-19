"""
Manage the global config from a file for the projects

The configuration file is searched for in the following locations by default:

* environ['EMGOAT_CONFIG']
* /etc/emgoat-cluster.conf
* /usr/local/etc/emgoat-cluster.conf
* $HOME/.emgoat-cluster.conf
"""

import os
import configparser


def get_config():
    """
    Return the configuration, read from one of the expected locations.
    """
    # this is the order where how we read the file
    # the conf file under home is with most priority
    # then it's in /etc/
    # finally it's the /usr/local one
    #
    # this order enables the testing easily
    inputs = [
        os.environ.get('EMGOAT_CONFIG', ''),
        os.path.expanduser('~/.emgoat-cluster.conf'),
        '/etc/emgoat-cluster.conf',
        '/usr/local/etc/emgoat-cluster.conf'
    ]

    for inputFile in inputs:
        if inputFile and os.path.exists(inputFile) and os.path.isfile(inputFile):
            return load_config(inputFile)

    raise IOError('No valid configuration file found on the node')


def load_config(filename):
    """ Load a configuration file, raise an Exception if filename is None. """
    if filename is None:
        raise IOError(f"Can not load configuration from None")

    if not os.path.exists(filename) or not os.path.isfile(filename):
        raise IOError(f"Input file {filename} does not exists or "
                      f"it is not a valid file. ")

    config = configparser.ConfigParser()
    config.read(filename)
    return config


# Global config variables for the emgoat module
config = get_config()


def use_slack():
    """
    here we will read from the config to see whether we will use slack or not
    :return: True if we use slack, otherwise false returned
    """
    from .util import Config
    return Config(config['slack']).get_bool('use_slack')

