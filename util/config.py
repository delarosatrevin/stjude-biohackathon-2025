"""
Manage the config from a file for the projects

The configuration file is searched for in the following locations by default:

* /etc/emgoat-cluster.conf
* /usr/local/etc/emgoat-cluster.conf
* $HOME/.emgoat-cluster.conf
"""
import configparser
import os
import os.path

# global variable to hold loaded configuration
CONFIG = None

def get_config(filename=None):
    """
    Return the configuration, and load from a file if the
    configuration has not already been loaded.
    """
    global CONFIG
    if CONFIG is not None:
        return CONFIG

    # initialize
    CONFIG = configparser.ConfigParser()

    # read in the file
    if filename is None:

        # this is the order where how we read the file
        # the conf file under home is with most priority
        # then it's in /etc/
        # finally it's the /usr/local one
        #
        # this order enables the testing easily
        inputs = [
            os.path.expanduser('~/.emgoat-cluster.conf'),
            '/etc/emgoat-cluster.conf',
            '/usr/local/etc/emgoat-cluster.conf'
        ]

        # now check the path and read in
        read_the_file = False
        for conf_file in inputs:
            if os.path.isfile(conf_file):
                CONFIG.read(conf_file)
                read_the_file = True
                break

        # so we should have the conf file loaded
        # double check here
        if not read_the_file:
            raise IOError('No valid configuration file found on the node')
    else:

        # check whether the file exist
        if os.path.isfile(filename):
            CONFIG.read(filename)
        else:
            print("The input conf file name is: {}".format(filename))
            raise IOError('No valid configuration file passed in the function, double check file name above')

    # final return
    return CONFIG

def use_slack():
    """
    here we will read from the config to see whether we will use slack or not
    :return: True if we use slack, otherwise false returned
    """
    config = get_config()
    use_slack_val = config['slack']['use_slack'].strip().lower()
    if use_slack_val == "0" or use_slack_val == "false":
        return False
    elif use_slack_val == "1" or use_slack_val == "true":
        return True
    else:
        raise RuntimeError("The value set in config['slack']['use_slack'] is "
                           "invalid: {}".format(use_slack_val))

def get_config_option(option_value: str):
    """
    this function is used to jude whether the configure option is True or False

    In the configuration file, for true or false option; you can either use 0 or 1; or true or false
    to represent it; other values are not allowed
    :param option_value: the original option value we get from config
    :return: True if the option is set true
    """
    val = option_value.lower().strip()
    if val == "0" or val == "false":
        return False
    elif val == "1" or val == "true":
        return True
    else:
        raise RuntimeError("Invalid configure option passed in: {}".format(option_value))
