#
# ldap service
#
# also NTLM is better for security purpose, however it requires MD4 and break the package
# dependencies. So we switch to simple algorithm instead.
#
from emgoat.config import get_config
from ldap3 import Server, Connection, ALL
from ldap3.utils.conv import escape_filter_chars

##########################
# get the global conf
##########################
CONFIG = get_config()

class LDAPUtil:
    """
    this class initialize the connection for LDAP server
    """

    # data
    server = CONFIG['ldap']['server_address']
    search_base = CONFIG['ldap']['search_base']
    port = int(CONFIG['ldap']['port'])
    search_attributes = CONFIG['ldap']['search_attributes'].split()

    # we use this user to log into ldap server
    ldap_user = CONFIG['ldap']['user']
    ldap_user_passwd = CONFIG['ldap']['password']

    def __init__(self, user=None, passwd=None):
        """
        initialize the connection for LDAP server

        in the testing case we may need to pass in the user and passwd from outside
        the ldap section of user/pasword is just for place holder
        """

        # set up server
        ldap_server = Server(self.server, get_info=ALL, port=self.port)

        # connecting to the server
        if user is not None and passwd is not None:
            self.conn = Connection(server=ldap_server, user=user, password=passwd, auto_bind=True)
        else:
            self.conn = Connection(server=ldap_server, user=self.ldap_user, password=self.ldap_user_passwd, auto_bind=True)

        # connection
        if not self.conn.bind():
            print("Failed to connect to the AD server: {}".format(self.server))
            print(f"Error: {self.conn.result}")
            info = "For the LDAP it failed to get connection to the server, see the above error"
            raise RuntimeError(info)

        # in default we assume we always have result
        self.has_results = True

    def get_userinfor_data_through_email(self, email_address):
        """
        This function is to retrieve the user name (first name and last name) from an active LDAP connection
        :param email_address: the user's email address we want to do search
        :return: the corresponding user name, for example like Test, Mike; so the first name is Mike; last name is Test
        """

        # test whether the connection is still on
        if not self.conn.bind():
            print("Failed to connect to the AD server: {}".format(self.server))
            print(f"Error: {self.conn.result}")
            info = "For the LDAP it failed to get connection to the server, see the above error"
            raise RuntimeError(info)

        # define the search field
        # the email address could be in either mail or user principle name
        search_filter = "(|(mail={0})(userPrincipalName={1}))".format(escape_filter_chars(email_address),
                                                                      escape_filter_chars(email_address))

        # now doing the search
        self.has_results = self.conn.search(search_base=self.search_base, search_filter=search_filter,
                                   attributes=self.search_attributes)

        # it's possible that we do not have any results
        # if no results we just return
        if not self.has_results:
            return

        # we expect this returns only one user information
        # if not we need to generate an error
        if len(self.conn.entries) != 1:
            print(self.conn.entries)
            info = ("For the output ldap3 results it seems multiple users "
                    "matches the given email: {}").format(email_address)
            raise RuntimeError(info)

    def get_user_names_from_user_infor(self):
        """
        this function should be used following the get_userinfor_data_through_email

        it will parse the result from the above function and return the user name
        in format of [first_name, last_name]
        """
        if self.has_results:
            return self.conn.entries[0].cn.value
        else:
            return "none"

    def get_hpc_account_name_from_user_infor(self):
        """
        this function should be used following the get_userinfor_data_through_email

        the hpc account name is defined in sAMAccountName
        """
        if self.has_results:
            return self.conn.entries[0].sAMAccountName.value
        else:
            return "none"

    def got_results(self):
        """
        whether the search has results
        """
        return self.has_results

    def close(self):
        """
        close the connection explicitly
        :return:
        """
        self.conn.unbind()