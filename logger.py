import os
from pathlib import Path
from datetime import datetime as dt
from functools import reduce

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

# class to print logs to console and/or file with multiple levels of criticality:
class logger:
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

    # initialize logger class object:
    def __init__(self,cli_logging_criticalities:list=[],file_logging_criticalities:list=[],log_path: Path=Path.cwd()/'default.log',delimiter='\t'):
        self.set_cli_logging_criticalities(cli_logging_criticalities)
        self.set_file_logging_criticalities(file_logging_criticalities)
        self.set_log_path(log_path)
        self.set_delimiter(delimiter)

    # print and/or log message according to specified criticality:
    def log(self,message,criticality='INFORMATION'):
        if criticality in self.criticalities.keys():
            if self.cli_logging_criticalities & self.criticalities[criticality]:
                print('{}: {}'.format(criticality,message))
            if self.file_logging_criticalities & self.criticalities[criticality]:
                with open(self.log_path,'a') as f:
                    t = dt.now().isoformat()
                    entry = '{}{}{}:{}{}\n'.format(t,self.delimiter,criticality,self.delimiter,message)
                    f.write(entry)

    # set criticality levels at which messages will be reported to the command line
    # interface:
    def set_cli_logging_criticalities(self,l: list):
        self.cli_logging_criticalities = reduce(lambda a,b:a|b,[self.criticalities[s] if s in self.criticalities.keys() else 0 for s in l],0b111)

    # set criticality levels at which messages will be saved to the log file:
    def set_file_logging_criticalities(self,l: list):
        self.file_logging_criticalities = reduce(lambda a,b:a|b,[self.criticalities[s] if s in self.criticalities.keys() else 0 for s in l],0b111)

    # set the location to save the log file--the file created if it does not
    # exist, and is otherwise appended:
    def set_log_path(self,p: Path):
        if p.is_file():
            self.log_path = p
        else:
            try:
                with p.open(mode='w') as f:
                    pass
                self.log_path = p
            except:
                self.set_file_logging_criticalities([])
                self.log_path = Path.cwd() / 'default.log'

    # set the delimiter between sections of the message when saving to the log
    # file, e.g., if saving as a .csv file:
    def set_delimiter(self,s: str):
        self.delimiter=s