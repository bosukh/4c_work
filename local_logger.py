import logging
from datetime import datetime

import os

# Trying something

# Added on July 19, 2016: Singleton construct
class Singleton(object):
    _instance = None

    def __new__(class_, *args, **kwargs):

        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance
## -- END OF SINGLETON CLASS -- ##

# Added on July 19, 2016: Base logger class
class LocalBaseLogger(Singleton):
    log_basedir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')
    date_suffix_fmt = '%Y_%m_%d'

    # Initialization Method
    def __new__ (cls, logsubdir=None):

        # This will reset the Singleton (NOTE: Use cautiously)
        if logsubdir is not None:
            cls._instance = None

        # Get the class instance
        inst = super(LocalBaseLogger, cls).__new__(cls)

        # Check for log_basedir
        if not hasattr(inst, 'log_basedir'):
            # Create the log (base) directory if it does not exist
            try:
                os.makedirs(inst.log_basedir)
            except OSError:
                if not os.path.isdir(inst.log_basedir):
                    raise OSError('Cannot create the logs base directory [' + inst.log_basedir + '].')

        # Check for logdir
        if not hasattr(inst, 'logdir'):
            inst.logdir = os.path.join(inst.log_basedir, datetime.today().strftime(inst.date_suffix_fmt))
            # Create the log directory if it does not exist
            try:
                os.makedirs(inst.logdir)
            except OSError:
                if not os.path.isdir(inst.logdir):
                    raise OSError('Cannot create the logs directory [' + inst.logdir + '].')

            # This is to give extra flexibility of logging multiple entities for the same day
            if logsubdir is not None:
                inst.logdir = os.path.join(inst.logdir, logsubdir)
                # Check if the directory exists, else create it
                try:
                    os.makedirs(inst.logdir)
                except OSError:
                    if not os.path.isdir(inst.logdir):
                        raise OSError('Cannot create the logs directory [' + inst.logdir + '].')
        return inst
    ## -- END OF INITIALIZATION METHOD -- ##
## -- END OF CLASS -- ##

# New Local Logger Class
# Added on July 20, 2016: Enabled with NullHandler
class LocalLogger(object):

    date_suffix_fmt = '%Y_%m_%d'

    # Initialization method
    # Added on July 19, 2016: logsubdir is to give extra flexibility of logging
    def __init__(self, logger_name=None, logfile=None, logsubdir=None):

        # If there is Exception
        self.err_msg = None

        # Create a base logger object which sets up the log dictories
        base_logger_obj = None
        try:
            base_logger_obj = LocalBaseLogger(logsubdir=logsubdir)
        except Exception as exc:
            #GOLD#raise ValueError('Initialization of Base Logger object failed.')
            self.err_msg = 'Initialization of Base Logger object failed.'

        # Get the logdir from base logger
        self.logdir = base_logger_obj.logdir

        #TODO## Copy the date suffix format from base logger
        #TODO#self.date_suffix_fmt = self.base_logger_obj.date_suffix_fmt

        # Instantiate the logger
        if logger_name:
            self.logger = logging.getLogger(logger_name)
        else:
            #GOLD#raise ValueError('Logger Name cannot be None.')
            self.err_msg = 'Logger Name cannot be None.'

        # This will make sure that the output goes to correct log file
        if logsubdir is not None:
            self.logger.handlers = list()

        # Instantiate the handler
        if logfile:
            # Updated on Sep 15, 2015:
            # This will make sure that if the handle is already assigned, it wont add the same handle twice
            # NOTE: If same handler is added multiple times, you will end up writing multiple lines for the same thing in the same file
            if self.logger.handlers:
                self.handler = self.logger.handlers[0]
            else:
                self.handler = logging.FileHandler(os.path.join(self.logdir, logfile))
        else:
            #GOLD#raise ValueError('Logger File cannot be None')
            self.err_msg = 'Logger File cannot be None'

        # If there was an error instantiate a Null Handler
        if self.err_msg is None:
            self.formatter = logging.Formatter(fmt='[%(levelname)s] -- %(asctime)s :: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
            self.handler.setFormatter(self.formatter)
            self.logger.addHandler(self.handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logging.getLogger(__name__)
            self.handler = logging.NullHandler()
            self.logger.addHandler(self.handler)
        # END IF

    # -- END OF INITIALIZATION METHOD -- #
