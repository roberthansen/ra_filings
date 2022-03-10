import pandas as pd
from pathlib import Path
from datetime import datetime as dt
from functools import reduce

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

class TextLogger:
    '''
    messages can be logged to either the command line interface, a specified
    log file, or both. both log targets include instance variables to define
    whether messages of a given level of criticality will be logged:
    'INFORMATION', 'WARNING', and 'ERROR'. each message thus includes a level
    of criticality, defaulting to 'INFORMATION'. logging levels are defined for
    log targets as well as messages using three bits and applied through bit-
    masking according to the following schematic:
        __0 - INFORMATION logging off
        __1 - INFORMATION logging on
        _0_ - WARNING logging off
        _1_ - WARNING logging on
        0__ - ERROR logging off
        1__ - ERROR logging on
    while log targets may accept any combination of on and off, messages must
    have exactly one bit on.
    '''
    
    # class variable to define levels of criticality, used for specifying the
    # criticality of a message:
    criticalities = {
        'INFORMATION' : 0b001,
        'WARNING' : 0b010,
        'ERROR' : 0b100,
    }

    # delimiter symbol used when saving messages to a log file:
    delimiter = ','

    def __init__(self,cli_logging_criticalities:list=[],file_logging_criticalities:list=[],log_path:Path=Path.cwd()/'default.log'):
        '''
        initializes an instance of the message_logger class.

        parameters:
            cli_logging_criticalities - a list of message criticalities which will be logged to the command line interface
            file_logging_criticalities - a list of message criticalities which will be logged to file
            log_path - a path object pointing to the file to which logs will be saved
        '''
        self.set_cli_logging_criticalities(cli_logging_criticalities)
        self.set_file_logging_criticalities(file_logging_criticalities)
        self.set_log_path(log_path)

    def log(self,message,criticality='INFORMATION'):
        '''
        logs a message with the given criticality to either the file or command
        line interface according to the criticalities set at the object level.

        parameters:
            message - a string message to send to the log outputs
            criticality - a string indicating the level of criticality for the
                message, must match one of the keys of the criticalities dict
        '''
        if criticality in self.criticalities.keys():
            if self.cli_logging_criticalities & self.criticalities[criticality]:
                print('{}: {}'.format(criticality,message))
            if self.file_logging_criticalities & self.criticalities[criticality]:
                with open(self.log_path,'a') as f:
                    t = dt.now().isoformat()
                    entry = '{}{}{}{}{}\n'.format(t,self.delimiter,criticality,self.delimiter,message)
                    f.write(entry)

    def set_cli_logging_criticalities(self,criticalities:list):
        '''
        sets the criticalities to apply when logging to the command-line
        interface.

        parameters:
            criticalities - a list of strings, each of which must match a key
                of the criticalities dict
        '''
        self.cli_logging_criticalities = reduce(lambda a,b:a|b,[self.criticalities[s] if s in self.criticalities.keys() else 0 for s in criticalities],0b000)

    def set_file_logging_criticalities(self,criticalities:list):
        '''
        sets the criticalities to apply when logging to the log file.

        parameters:
            criticalities - a list of strings, each of which must match a key
                of the criticalities dict
        '''
        self.file_logging_criticalities = reduce(lambda a,b:a|b,[self.criticalities[s] if s in self.criticalities.keys() else 0 for s in criticalities],0b000)

    def set_log_path(self,log_path:Path):
        '''
        sets the path of the file to which messages will be logged according to
        criticality; if the file does not exist, it will be created when writing
        the first message.

        parameters:
            log_path - path object pointing to a file where log messages will be saved
        '''
        try:
            if not log_path.is_file():
                with log_path.open(mode='w') as f:
                    pass
            self.log_path = log_path
        except:
            self.set_file_logging_criticalities([])
            self.log_path = Path.cwd() / 'default.log'

class DataLogger:
    '''
    data can be logged to a specified csv file. this class either loads data
    from an existing file, or creates a new log file. data is represented
    internally as a pandas dataframe, which is saved to file upon request.
    '''
    # initialize logger class object:
    def __init__(self,columns:list,log_path:Path=Path.cwd()/'default.csv',delimiter:str='\t'):
        '''
        initializes an instance of the data_logger class. if the file specified
        in log_path exists, it is checked for data and, if data exists and the
        columns match the input list of columns, the contents are loaded into
        the internal data model. Otherwise, an empty pandas dataframe is
        initialized.

        parameters:
            log_path - path object pointing to the file to which data will be logged
            delimiter - delimiter to use when logging data
        '''
        self.columns = ['log_timestamp'] + columns
        self.set_log_path(log_path)
        self.set_delimiter(delimiter)

    def log(self,data:pd.Series):
        '''
        appends a single row of input data to the dataframe.

        parameters:
            data - a pandas series containing data to include in the log
        '''
        data['log_timestamp'] = dt.now()
        self.data = self.data.append(data,ignore_index=True)
    
    def commit(self):
        '''
        writes the log dataframe to file.
        '''
        self.data.to_csv(self.log_path,sep=self.delimiter,index=False)
    
    def load_log(self):
        '''
        checks the log file against the current list of columns and either
        loads the data or initializes a new dataframe.
        '''
        if self.log_path.is_file():
            file_data = pd.read_csv(self.log_path)
            if all([column in file_data.columns for column in self.columns]):
                self.columns = list(filter(lambda s: s!='log_timestamp',file_data.columns))
                self.data = file_data
            else:
                self.data = pd.DataFrame(columns=self.columns)
        else:
            self.data = pd.DataFrame(columns=self.columns)
        

    # set the location to save the log file--the file is created if it does
    # not already exist, and is otherwise overwritten:
    def set_log_path(self,log_path:Path):
        '''
        sets the path of the file to which messages will be logged according to
        criticality; if the file does not exist, it will be created when writing
        the first message.
        parameters:
            log_path - path object pointing to a file where log messages will be saved
        '''
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True,exist_ok=True)
        self.load_log()

    def set_delimiter(self,delimiter:str):
        '''
        set the delimiter between sections of a message when saving to the log
        file, e.g., if logging to a .csv file.
        parameters:
            delimiter - a string, typically of length 1, which will separate
                each section of messages when written to the log file
        '''
        self.delimiter=delimiter