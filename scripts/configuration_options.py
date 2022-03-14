import re
import glob
from pathlib import Path
from yaml import safe_load
from functools import reduce
import pandas as pd
from pandas import Timestamp as ts

class ConfigurationOptions:
    '''
    a class to handle configuration options defined in a yaml file
    '''
    def __init__(self,configuration_options_path:Path):
        '''
        initializes a ConfigurationOptions object and loads a configuration
        file, applying options.

        parameters:
            configuration_options_path - path object linking to a yaml file
                containing configuration options for the ra_consolidator class.
        '''
        self.options = {
            'filing_month' : None,
            'planning_reserve_margin' : 0,
            'organizations_filename' : None,
            'email_filter_filename' : None,
            'kiteworks_hostname' : None,
            'kiteworks_upload_folder' : None,
            'downloads_internal_directory' : None,
            'downloads_external_directory' : None,
            'ra_monthly_filing_filename' : None,
            'month_ahead_filename' : None,
            'cam_rmr_filename' : None,
            'year_ahead_filename' : None,
            'incremental_local_filename' : None,
            'supply_plan_system_filename' : None,
            'supply_plan_local_filename' : None,
            'ra_summary_filename' : None,
            'ra_summary_template_filename' : None,
            'caiso_cross_check_filename' : None,
            'caiso_cross_check_template_filename' : None,
            'results_archive_filename' : None,
            'webdriver_directory' : None,
            'browser' : 'firefox',
            'browser_action_timer' : 1.0,
            'browser_action_retries' : 5,
            'browser_headless' : 'no',
            'log_filename' : str(Path.cwd() / 'download_organizer.log'),
            'cli_logging_criticalities' : ['INFORMATION','WARNING','ERROR'],
            'file_logging_criticalities' : ['INFORMATION','WARNING','ERROR'],
            'email_log_filename' : None,
            'attachment_log_filename' : None,
            'consolidation_log_filename' : None,
        }
        self.load_configuration_options(configuration_options_path)
    def load_configuration_options(self,configuration_options_path:Path):
        '''
        reads configuration file and applies options.

        parameters:
            configuration_options_path - path object linking to a yaml file
                containing configuration options for the ra_consolidator class.
        '''
        if configuration_options_path.is_file():
            self.configuration_path = configuration_options_path
            with self.configuration_path.open(mode='r') as f:
                d = safe_load(f.read())
            for key in d.keys():
                if key in self.options.keys():
                    value = d[key]
                    if 'criticalities' in key:
                        self.options[key] = value.split(',')
                    elif key=='filing_month':
                        self.options[key] = pd.to_datetime(value)
                    else:
                        self.options[key] = value
        else:
            self.configuration_path = None
    def get_option(self,option_name:str):
        '''
        provides parametric access to the options dictionary.

        parameters:
            option_name - a string matching a key in the object's options
                dictionary
        '''
        if option_name in self.options.keys():
            option = self.options[option_name]
        else:
            option = None
        return option

class Paths:
    '''
    a class to handle paths and path template strings defined in the
    configuration options yaml file
    '''
    def __init__(self,configuration_options:ConfigurationOptions):
        '''
        populates the paths instance variable with parsed path objects pointing
        to each file, based on the configuration options instance variable.

        parameters:
            configuration_options - an instance of the ConfigurationOptions class
        '''
        self.filing_month = configuration_options.get_option('filing_month')
        self.path_strings = {
            'organizations' : configuration_options.get_option('organizations_filename'),
            'email_filter' : configuration_options.get_option('email_filter_filename'),
            'downloads_internal' : configuration_options.get_option('downloads_internal_directory'),
            'downloads_external' : configuration_options.get_option('downloads_external_directory'),
            'ra_monthly_filing' : configuration_options.get_option('ra_monthly_filing_filename'),
            'month_ahead' : configuration_options.get_option('month_ahead_filename'),
            'cam_rmr' : configuration_options.get_option('cam_rmr_filename'),
            'year_ahead' : configuration_options.get_option('year_ahead_filename'),
            'incremental_local' : configuration_options.get_option('incremental_local_filename'),
            'supply_plan_system' : configuration_options.get_option('supply_plan_system_filename'),
            'supply_plan_local' : configuration_options.get_option('supply_plan_local_filename'),
            'ra_summary' : configuration_options.get_option('ra_summary_filename'),
            'ra_summary_template' : configuration_options.get_option('ra_summary_template_filename'),
            'caiso_cross_check' : configuration_options.get_option('caiso_cross_check_filename'),
            'caiso_cross_check_template' : configuration_options.get_option('caiso_cross_check_template_filename'),
            'results_archive' : configuration_options.get_option('results_archive_filename'),
            'webdrivers' : configuration_options.get_option('webdriver_directory'),
            'log' : configuration_options.get_option('log_filename'),
            'email_log' : configuration_options.get_option('email_log_filename'),
            'attachment_log' : configuration_options.get_option('attachment_log_filename'),
            'consolidation_log' : configuration_options.get_option('consolidation_log_filename'),
        }
        self.version_controlled_files = [
            'ra_monthly_filing',
            'cam_rmr',
            'month_ahead',
            'year_ahead',
            'incremental_local',
            'supply_plan_system',
            'supply_plan_local',
        ]
    def parse_filename(self,filename:str,organization:dict=None,date:ts=None,version:int=0):
        '''
        parses a filename string and resolves tokens with replacement values
        based on input parameters. returns path objects defined relative to the
        current working directory.

        parameters:
            filename - a string to be resolved into a path after replacing any
                tokens with values based on other parameters
            organization - a dictionary containing information about a single
                organization
            date - date to use when replacing date-based tokens
            version - a version number to use with version-controlled files
        '''
        parsed_filename = filename
        if organization is None:
            organization = {
                'id' : '',
                'name' : ''
            }
        else:
            pass
        if pd.isnull(date):
            date = self.filing_month
        else:
            pass
        replacements = {
            '[yy]' : pd.to_datetime(date).strftime('%y'),
            '[yyyy]' : pd.to_datetime(date).strftime('%Y'),
            '[mm]' : pd.to_datetime(date).strftime('%m'),
            '[mmm]' : pd.to_datetime(date).strftime('%b'),
            '[mmmm]' : pd.to_datetime(date).strftime('%B'),
            '[organization_id]' : organization['id'],
            '[organization_name]' : organization['name'],
            '[version]' : '{:02.0f}'.format(version),
        }
        for token in re.findall(r'\[\w[_A-Za-z]*\]',filename):
            if token in replacements.keys():
                parsed_filename = parsed_filename.replace(token,replacements[token])
            else:
                pass
        return Path(parsed_filename).relative_to(Path.cwd())
    def get_path(self,path_id:str,organization:dict=None,date:ts=None,version:int=None):
        '''
        provides parametric access to the path_strings dictionary with parsing.

        parameters:
            path_id - a string matching a key in the object's paths dictionary
            organization - a dictionary containing information about a single
                organization from the organizations.yaml file
            date - a datetime applied to certain paths
            version - an integer identifying a specific revision of a version-
                controlled file applied to certain paths
        '''
        if date is None:
            date = self.filing_month
        else:
            pass
        if path_id in self.path_strings.keys():
            if path_id=='ra_monthly_filing':
                if organization is None:
                    path = None
                elif version is None:
                    path = self.most_recent_version(path_id,organization=organization,date=date)
                else:
                    path = self.parse_filename(self.path_strings[path_id],organization=organization,date=date,version=version)            
            elif path_id in self.version_controlled_files:
                if version is None:
                    path = self.most_recent_version(path_id,date=date)
                else:
                    path = self.parse_filename(self.path_strings[path_id],date=date,version=version)
            else:
                path = self.parse_filename(self.path_strings[path_id],date=date)
        elif path_id=='ra_summary_previous_month':
            previous_date = date.replace(year=date.year-int((13-date.month)/12),month=(date.month+10)%12+1)
            path = self.parse_filename(self.path_strings['ra_summary'],date=previous_date)
        else:
            path = None
        return path
    def most_recent_version(self,path_id:str,organization:dict=None,date:ts=None):
        '''
        searches directories for all versions of files matching a specified
        filename template, returning a path object pointing to the file with
        the highest version number.

        parameters:
            path_id - a string matching a key in the object's paths dictionary
            organization - a dictionary containing information about a single
                organization
            date - date to use when replacing date-based tokens
        '''
        if path_id in self.version_controlled_files:
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename.replace('[version]','[_version_]'),organization,date)
            versions = glob.glob(str(path).replace('[_version_]','[0-9][0-9]'))
            versions.sort(reverse=True)
            if len(versions)>0:
                path = Path(versions[0])
            else:
                path = None
        elif path_id in self.path_strings.keys():
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename,organization,date)
        else:
            path = None
        return path
    
    def get_all_versions(self,path_id:str,organization:dict=None,date:ts=None):
        '''
        searches directories and returns a list of all versions of files
        matching a specified filename template.

        parameters:
            path_id - a string matching a key in the object's paths dictionary
            organization - a dictionary containing information about a single
                organization
            date - date to use when replacing date-based tokens
        '''
        if path_id in self.version_controlled_files:
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename.replace('[version]','[_version_]'),organization,date)
            versions = glob.glob(str(path).replace('[_version_]','[0-9][0-9]'))
            versions.sort(reverse=True)
            paths = [Path(version) for version in versions]
        else:
            paths = None
        return paths


class Organizations:
    '''
    a class to handle organization information defined in a yaml file
    '''
    def __init__(self,organizations_path:Path):
        '''
        reads a yaml file with information about organizations, and provides
        methods for accessing the information.

        parameters:
            organization_path - path object pointing to organization
                information yaml file
        '''
        if organizations_path.is_file():
            self.path = organizations_path
            with self.path.open(mode='r') as f:
                self.data = safe_load(f)
            self.alias_map = dict((key,self.data[key]['aliases']) for key in self.data.keys())
        else:
            self.path = None
            self.data = dict()
            self.alias_map = dict()

    def lookup_id(self,alias:str):
        '''
        returns the default organization abbreviation for a valid input alias.

        parameters:
            alias - an alias mapped to an organization id
        '''
        if alias in [value for values in self.alias_map.values() for value in values]:
            filter_function = lambda id: alias in self.alias_map[id]
            organization_id = next(filter(filter_function,self.alias_map.keys()))
        elif alias in self.alias_map.keys():
            organization_id = alias
        else:
            organization_id = ''
        return organization_id
    
    def get_type(self,organization_id:str):
        '''
        returns the type of organization for a given organization_id.

        parameters:
            organization_id - the identifier defined as a key in the
                organizations yaml file
        '''
        if organization_id in self.data.keys():
            organization_type = self.data[organization_id]['type']
        else:
            organization_type = 'Unknown'
        return organization_type

    def get_aliases(self,organization_id:str):
        '''
        returns a list of all known aliases for a given organization_id.

        parameters:
            organization_id - the identifier defined as a key in the
                organizations yaml file
        '''
        if organization_id in self.alias_map.keys():
            aliases = self.alias_map[organization_id]
        else:
            aliases = []
        return aliases
    
    def get_name(self,organization_id:str):
        '''
        returns the first-listed alias for a given organization_id.

        parameters:
            organization_id - the identifier defined as a key in the
                organizations yaml file
        '''
        if organization_id in self.alias_map.keys():
            alias = self.alias_map[organization_id][0]
        else:
            alias = ''
        return alias
    
    def get_organization(self,organization_id:str):
        '''
        returns all information about a single organization as a dictionary.

        parameters:
            organization_id - the identifier defined as a key in the
                organizations yaml file
        '''
        if organization_id in self.data.keys():
            organization = self.data[organization_id]
            organization['id'] = organization_id
            organization['name'] = self.get_name(organization_id)
        else:
            organization = dict()
        return organization

    def list_organization_ids(self):
        '''
        provides a list of all organization ids
        '''
        return self.data.keys()
    
    def list_load_serving_entities(self):
        '''
        provides a list of all organizations identified as either load-serving
        entities or investor-owned utilities.
        '''
        load_serving_entities = [
            self.get_organization(organization_id) \
            for organization_id in self.data.keys() \
            if self.get_type(organization_id) in (
                'load-serving entity','investor-owned utility'
            )
        ]
        return load_serving_entities
    
    def list_all_aliases(self):
        '''
        provides a flat list of aliases for all organizations.
        '''
        aliases = [alias for aliases in self.alias_map.values() for alias in aliases]
        return aliases
    
class EmailFilter:
    '''
    a class to handle email filtering based on a yaml file
    '''
    def __init__(self,email_filter_path:Path):
        '''
        initializes an email filter object, reads the email filter yaml file,
        and populates the include and exclude keyword lists.

        parameters:
            email_filter_path - a path object pointing to the yaml file
                containing keywords to include and exclude when filtering
                emails based on their subject lines.
        '''
        self.keywords = {
            'include' : [],
            'exclude' : [],
        }
        if email_filter_path.is_file():
            self.email_filter_path = email_filter_path
            self.keywords = dict()
            with self.email_filter_path.open(mode='r') as f:
                d = safe_load(f)
                for key in d.keys():
                    value = d[key]
                    if value is None:
                        value = []
                    else:
                        pass
                    if key.lower() in ('include', 'exclude'):
                        self.keywords[key.lower()] = [s.lower() for s in value]
                    else:
                        pass
        else:
            self.email_filter_path = None
    
    def check_email_subject(self,email_subject:str):
        '''
        compares an input email subject string against the lists of keywords to
        include and exclude, returning a boolean indicating whether the email
        subject passes both checks.

        parameters:
            email_subject - a string, i.e., the text of an email subject, to be
                compared against the include and exclude keyword lists
        '''
        if isinstance(self.keywords['include'],list):
            include = reduce(lambda x,y:x|y,[s in email_subject.lower() for s in self.keywords['include']],False)
        else:
            include = True
        if isinstance(self.keywords['exclude'],list):
            exclude = reduce(lambda x,y:x|y,[s in email_subject.lower() for s in self.keywords['exclude']],False)
        else:
            exclude = False
        include_email = include and not exclude
        return include_email