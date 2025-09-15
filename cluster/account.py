"""
this file defines the tables for the database

The department and node tables are very simple, therefore we do not have
functions to update the data directly. Instead during the account creation process
(for either normal account or the cryo account), we will check the department and
node information and update the database accordingly.

The PI information should be filled before the corresponding account is added in.
Please run the add_new_pi function before adding the corresponding account.
"""
from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column

class Base(DeclarativeBase):
    """
    this is the base class for all the tables defined below
    all the data tables are derived from this base class
    this base class contains the metadata for the sql tables
    """
    pass

class Departments(Base):
    """
    define the table for the department
    """
    __tablename__ = "departments"

    department: Mapped[str] = mapped_column(primary_key=True, nullable=False)

class Node(Base):
    """
    define the virtual machine/Node for cryosparc running
    """
    __tablename__ = "node"

    node: Mapped[str] = mapped_column(primary_key=True, nullable=False)

class PI(Base):
    """
    define the table for the pi

    for each PI we also have an short name (in variable name) as used in other tables
    """
    __tablename__ = "pi"

    first_name:  Mapped[str] = mapped_column(nullable=False)
    last_name:   Mapped[str] = mapped_column(nullable=False)
    name:        Mapped[str] = mapped_column(nullable=False)
    email:       Mapped[str] = mapped_column(nullable=False, primary_key=True)


class Accounts(Base):
    """
    define the table for general HPC accounts, this can be a cryosparc account
    or non-cryo account

    the purpose of account is mainly used for associating with Jobs

    each account has an PI, possible a few of delegates; and a group of users;
    each account is from a given department
    """
    __tablename__ = "accounts"

    name: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    pi_email = mapped_column(ForeignKey("pi.email"), nullable=False)
    department = mapped_column(ForeignKey("departments.department"), nullable=False)

class CRYO_Accounts(Base):
    """
    define the table for cryosparc accounts

    each account has an PI, possible a few of delegates; and a group of users;
    each account is from a given department

    from technical details, the account is associated with a VM/node, and the home path to
    host the database and software on the VM/node

    each account may also have it's own storage path (shared storage like GPFS) and
    it's defined backup path.

    usually the storage path here refers to the input/output data locations for cryosparc service.

    cryo_master_path is the installed cryosparc master software path for the account on the VM/node

    similarly, the worker path is the installed worker software path

    database_path is where the mongoDB database stored on the vm/node

    port is the designed starting port number for the account

    portal path is the web link for accessing the portal, currently we use http protocol;
    and port number need to be used
    """
    __tablename__ = "cryo_accounts"

    name:                 Mapped[str] = mapped_column(ForeignKey("accounts.name"), primary_key=True, nullable=False)
    home:                 Mapped[str] = mapped_column(nullable=False)
    storage_path:         Mapped[str] = mapped_column(nullable=False)
    backup_path:          Mapped[str] = mapped_column(nullable=False)
    database_path:        Mapped[str] = mapped_column(nullable=False)
    portal_path:          Mapped[str] = mapped_column(nullable=False)
    cryo_master_path:     Mapped[str] = mapped_column(nullable=False)
    cryo_worker_path:     Mapped[str] = mapped_column(nullable=False)
    pi_email   = mapped_column(ForeignKey("pi.email"), nullable=False)
    department = mapped_column(ForeignKey("departments.department"), nullable=False)
    node = mapped_column(ForeignKey("node.node"), nullable=False)

class ErrLogs(Base):
    """
    define the logs for error for all of cryosparc services (each cryosparc service for one account)
    """
    __tablename__ = "errlogs"

    account: Mapped[str] = mapped_column(ForeignKey("cryo_accounts.name"), primary_key=True, nullable=False)
    time: Mapped[datetime] = mapped_column(primary_key=True, nullable=False)
    log: Mapped[str] = mapped_column(nullable=False)

class Admins(Base):
    """
    define the table for admin users, admin users are managing the cryosparc accounts on all VMs/Nodes
    """
    __tablename__ = "admins"

    hpc_account: Mapped[str] = mapped_column(nullable=True)
    first_name:  Mapped[str] = mapped_column(nullable=False)
    last_name:   Mapped[str] = mapped_column(nullable=False)
    email:       Mapped[str] = mapped_column(nullable=False, primary_key=True)

class Users(Base):
    """
    define the table for users, each user will be from a given account

    a user can be a delegate for an account, that means the user has
    the admin privilege for managing the account

    the user hpc account is the user's HPC account name, it could be null because
    the user may not have the hpc account

    the user name is the name the user used in system

    each user may also have other collaborative accounts, which are secondary and third
    account and fourth account

    the collaborative accounts means the user can use these account to submit jobs, or
    able to access their storage data
    """
    __tablename__ = "users"

    hpc_account: Mapped[str] = mapped_column(nullable=True)
    first_name:  Mapped[str] = mapped_column(nullable=False)
    last_name:   Mapped[str] = mapped_column(nullable=False)
    name:        Mapped[str] = mapped_column(nullable=False, primary_key=True)
    email:       Mapped[str] = mapped_column(nullable=False, primary_key=True)
    is_delegate: Mapped[bool] = mapped_column(default=False)
    account = mapped_column(ForeignKey("accounts.name"), nullable=False)
    secondary_account = mapped_column(ForeignKey("accounts.name"), nullable=True)
    third_account = mapped_column(ForeignKey("accounts.name"), nullable=True)
    fourth_account = mapped_column(ForeignKey("accounts.name"), nullable=True)

class Jobs(Base):
    """
    define the table for cryosparc/non-cryosparc jobs

    the job ID is an integer; from LSF/slurm etc. However for easy handling we just treat it as a string

    csid is the job ID assigned by CryoSparc, it's a string; if this is not a cryo job it's None

    workspace id is the job ID for workspace, it's a string. In default it's not needed, since a lot of
    case we can not capture workspace id from the output

    project id is similar with workspace id, it's for project. If this is not a cryo job it's None

    the state is whether it's pending/running/finished/error/cancelled (five states allowed), the state
    symbol is taken from the scheduler (lsf/slurm)

    the general state is our internal defined state symbol (see util.py, the JOB_STATUS_PD etc.)

    if the job has an error, the error will have the corresponding error log

    pending time is how long the job is pending (in unit of minutes)

    job used time: how much time the job is used (wall time), in unit of minutes

    start time: the job starting time (dateTime)

    finish time: the job finish time (dateTime)

    memory used: how much memory used in unit of GB

    cpu/gpu used: how many cores/gpus used for the cryosparc job

    submit time: when the job is submitted, this is a primary key and we use it to sort jobs

    cpu efficiency: sometimes scheduler provides the efficiency used by the job, this is the
    efficiency used by cpu cores

    mem efficiency: sometimes scheduler provides the efficiency used by the job, this is the
    efficiency used by memeory requested

    compute nodes are the nodes that the job are submitted onto.

    usually for cryo job it's submitted in terms of the account name, so account name is
    necessary; but the user who submit the job is difficult to check. We made it as
    unnecessary data field.

    for a lot of data fields like pending time and job used time etc. we need to update them
    after the job is running or finished. Since the job record is estalished when the job is
    pending, so we allow these data fields to be nullable.
    """
    __tablename__ = "jobs"

    workspaceid:    Mapped[str] = mapped_column(nullable=True)
    projectid:      Mapped[str] = mapped_column(nullable=True)
    jobid:          Mapped[str] = mapped_column(primary_key=True, nullable=False)
    csid:           Mapped[str] = mapped_column(nullable=True)
    submit_time:    Mapped[datetime] = mapped_column(primary_key=True,nullable=False)
    state:          Mapped[str] = mapped_column(nullable=False)
    general_state:  Mapped[str] = mapped_column(nullable=False)
    error:          Mapped[str] = mapped_column(nullable=True)
    pending_time:   Mapped[int] = mapped_column(nullable=True)
    job_used_time:  Mapped[int] = mapped_column(nullable=True)
    start_time:     Mapped[datetime] = mapped_column(nullable=True)
    finish_time:    Mapped[datetime] = mapped_column(nullable=True)
    cpu_used:       Mapped[int] = mapped_column(nullable=True)
    gpu_used:       Mapped[int] = mapped_column(nullable=True)
    memory_used:    Mapped[int] = mapped_column(nullable=True)
    cpu_efficiency: Mapped[float] = mapped_column(nullable=True)
    mem_efficiency: Mapped[float] = mapped_column(nullable=True)
    gpu_type:       Mapped[str] = mapped_column(nullable=True)
    compute_nodes:  Mapped[str] = mapped_column(nullable=True)
    user = mapped_column(ForeignKey("users.name"), nullable=True)
    account_name = mapped_column(ForeignKey("accounts.name"), primary_key=True, nullable=False)

    def is_a_cryo_job(self):
        """
        whether this is a cryosparc job?

        We test the csid and project id, for non-cryosparc job either of them
        are just None.
        :return: true if it's a cryosparc job
        """
        if not self.csid or not self.projectid:
            return False
        else:
            return True

