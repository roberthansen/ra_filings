import csv
import io
import os
import re
import shutil
from yaml import safe_load
from pathlib import Path
from openpyxl import load_workbook
from zipfile import ZipFile
from logger import logger

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

# class to open excel files within a specified directory and copy files
# recognized as Resource Adequacy Monthly Filings to a separate directory with
# a new name according to the filing contents.
class ra_filing_organizer:
    # initialize download_organizer class object:
    def __init__(self,configuration_path: Path=Path.cwd()):
        # configuration options with default values:
        self.configuration_options = {
            'lse_map_file' : None,
            'temp_directory' : Path.cwd(),
            'ra_monthly_filing_filename_template' : None,
            'log_file' : Path.cwd() / 'download_organizer.log',
            'cli_logging_criticalities' : ['INFORMATION','WARNING','ERROR',],
            'file_logging_criticalities' : ['INFORMATION','WARNING','ERROR',],
        }
        self.logger = logger()
        self.lse_map = dict()
        self.set_configuration_options(configuration_path)

    # set filename template:
    def set_ra_monthly_filing_filename_template(self,s):
        self.configuration_options['ra_monthly_filing_filename_template'] = s

    # set directory for input files, i.e., downloaded kiteworks email
    # attachments:
    def set_input_directory(self,p: Path):
        if p.is_dir():
            self.input_directory = p
            self.logger.log('Input Directory Set to {}'.format(p),'INFORMATION')
        else:
            self.input_directory = Path.cwd()
            self.logger.log('Input Directory Not Found at {} --- Using {}'.format(self.input_directory,Path.cwd()),'WARNING')

    # set directory to which renamed files will be copied:
    def set_output_directory(self,p: Path):
        if p.is_dir():
            self.output_directory = p
            self.logger.log('Output Directory Set to {}'.format(p),'INFORMATION')
        else:
            self.output_directory = Path.cwd()
            self.logger.log('Output Directory Not Found at {} --- Using {}'.format(p,Path.cwd()),'WARNING')

    # read file with mapping from load serving entities' full names and
    # variations to abbreviations:
    def set_lse_map(self,p: Path):
        if p.is_file():
            self.lse_map_file = p
            with self.lse_map_file.open(mode='r') as f:
                d = safe_load(f)
            # flip keys and list values
            self.lse_map = dict()
            for key in d.keys():
                for value in d[key]:
                    self.lse_map[value] = key
            self.logger.log('Loaded LSE Map from {}'.format(p),'INFORMATION')
        else:
            self.lse_map_file = None
            self.logger.log('No LSE Mapping File Found at {}'.format(p),'ERROR')

    # read configuration file and apply relevant options:
    def set_configuration_options(self,p: Path):
        if p.is_file():
            self.configuration_path = p
            with self.configuration_path.open(mode='r') as f:
                d = safe_load(f.read())
            for key in d.keys():
                if key in self.configuration_options.keys():
                    value = d[key]
                    if 'criticalities' in key:
                        self.configuration_options[key] = value.split(',')
                    elif key=='ra_monthly_filing_filename_template':
                        self.configuration_options[key] = value
                    elif 'file' in key or 'directory' in key:
                        self.configuration_options[key] = Path(value)
                    else:
                        self.configuration_options[key] = value
            self.logger.log('Applying Configuration Options from {}'.format(p),'INFORMATION')
        else:
            self.configuration_path = None
            self.logger.log('Applying Default Configuration---Unable to Load Options from {}'.format(p),'WARNING')
        self.logger.set_log_path(self.configuration_options['log_file'])
        self.logger.set_cli_logging_criticalities(self.configuration_options['cli_logging_criticalities'])
        self.logger.set_file_logging_criticalities(self.configuration_options['file_logging_criticalities'])
        self.set_ra_monthly_filing_filename_template(self.configuration_options['ra_monthly_filing_filename_template'])
        self.set_input_directory(Path(self.configuration_options['temp_directory']))
        self.set_lse_map(Path(self.configuration_options['lse_map_file']))

    # open a single excel file and apply contents and filename template to save
    # to the out directory:
    def copy_rename(self,in_path: Path):
        if in_path.is_file():
            if in_path.suffix=='.xlsx':
                try:
                    self.logger.log('Opening File: {}'.format(in_path),'INFORMATION')
                    with open(in_path,'rb') as f:
                        in_mem_file = io.BytesIO(f.read())
                    wb = load_workbook(in_mem_file,read_only=True)
                    sheet = wb['Certification']
                    if sheet['B5'].value in self.lse_map.keys():
                        submittal_information = {
                            'lse_full' : sheet['B5'].value,
                            'lse_abbrev' : self.lse_map[sheet['B5'].value],
                            'lse_representative' : {
                                'name' : sheet['B21'].value,
                                'title' : sheet['B23'].value,
                                'email' : sheet['B22'].value,
                                'sign_date' : sheet['B24'].value,
                            },
                            'lse_contact' : {
                                'name' : sheet['B28'].value,
                                'title' : sheet['B29'].value,
                                'address' : '{}\n{}\n{}, {} {}'.format(sheet['B30'].value,sheet['B31'].value,sheet['B32'].value,sheet['B33'].value,sheet['B34'].value),
                                'phone' : sheet['B35'].value,
                                'email' : sheet['B36'].value,
                            },
                            'lse_backup_contact' : {
                                'name' : sheet['B40'].value,
                                'title' : sheet['B41'].value,
                                'phone' : sheet['B42'].value,
                                'email' : sheet['B43'].value,
                            },
                            'compliance_period' : sheet['B3'].value,
                            'submittal_date' : sheet['B7'].value,
                        }
                        out_path = self.out_filename(submittal_information)
                        os.makedirs(out_path.parent,exist_ok=True)
                        shutil.copyfile(in_path,out_path)
                        self.logger.log('Copying Filing from {} to {}'.format(in_path,out_path),'INFORMATION')
                    else:
                        self.logger.log('Load Serving Entity Name Not Found: \'{}\'\tAdd Name and Abbreviation to {}'.format(sheet['B5'].value,self.lse_map_file),'WARNING')
                    wb.close()
                except:
                    self.logger.log('Input Excel File Does Not Match Template: '.format(in_path),'INFORMATION')
            else:
                self.logger.log('Skipping {} File: {}'.format(in_path.suffix,in_path),'INFORMATION')
        else:
            self.logger.log('Input File Not Found: {}'.format(in_path),'WARNING')

    # apply the template in the config file or default to generate a filename
    # for copied files:
    def out_filename(self,submittal_information: dict):
        filename = self.configuration_options['ra_monthly_filing_filename_template']
        lse_abbrev = submittal_information['lse_abbrev']
        replacements = {
            '[yy]' : submittal_information['compliance_period'].strftime('%y'),
            '[yyyy]' : submittal_information['compliance_period'].strftime('%Y'),
            '[mm]' : submittal_information['compliance_period'].strftime('%m'),
            '[mmm]' : submittal_information['compliance_period'].strftime('%b'),
            '[mmmm]' : submittal_information['compliance_period'].strftime('%B'),
            '[lse_abbrev]' : lse_abbrev,
            '[lse_full]' : next(filter(lambda x: x[1]==lse_abbrev,self.lse_map.items()))[0],
        }
        for key in re.findall(r'\[\w[_A-Za-z]*\]',filename):
            if key in replacements.keys():
                filename = filename.replace(key,replacements[key])
            else:
                pass
        return Path(filename)

    # expand a single archive into the parent folder:
    def unzip(self,p: Path):
        if p.is_file():
            try:
                with ZipFile(p,'r') as z:
                    z.extractall(p.parent)
                self.logger.log('Decompressing {} Archive: {}'.format(p.suffix,p),'INFORMATION')
            except:
                self.logger.log('Unable to Decompress Archive','WARNING')
        else:
            self.logger.log('File Not Found: {}'.format(p),'WARNING')

    # tranverse directory tree, opening sub-directories recursively and copying/renaming files:
    def traverse(self,d: Path):
        if d.is_dir():
            for item in d.iterdir():
                if item.is_dir():
                    self.traverse(item)
                elif item.is_file():
                    self.copy_rename(item)
                else:
                    self.logger.log('Unknown Item: {}'.format(item),'WARNING')

    # remove all contents of the download directory:
    def cleanup(self,d: Path):
        if d.is_dir():
            for item in self.input_directory.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                elif item.is_file():
                    os.remove(item)
        elif d.is_file():
            os.remove(d)
    

    # find all files in the download directory and sub-directories to copy and rename:
    def organize(self):
        for item in self.input_directory.iterdir():
            if item.suffix=='.zip':
                self.unzip(item)
            else:
                pass
        self.traverse(self.input_directory)
        #self.cleanup(self.input_directory)