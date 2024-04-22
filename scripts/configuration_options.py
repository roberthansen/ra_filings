import re
import glob
from pathlib import Path
from yaml import safe_load
from functools import reduce
import pandas as pd
from pandas import Timestamp as ts,Timedelta as td

from california_state_holidays import california_state_holidays

class ConfigurationOptions:
    '''
    a class to handle configuration options defined in a yaml file
    '''
    def __init__(self,configuration_options_path:Path,filing_month:ts=None):
        '''
        initializes a ConfigurationOptions object and loads a configuration
        file, applying options.

        parameters:
            configuration_options_path - path object linking to a yaml file
                containing configuration options for the ra_consolidator class.
            filing_month - an optional filing month timestamp to overwrite the
                date in the configuration options yaml file
        '''
        self.options = {
            'filing_month' : filing_month,
            'planning_reserve_margin' : 0,
            'demand_response_multiplier' : 1,
            'demand_response_procurement_adder' : 0,
            'transmission_loss_adder_pge' : 1,
            'transmission_loss_adder_sce' : 1,
            'transmission_loss_adder_sdge' : 1,
            'organizations_filename' : None,
            'email_filter_filename' : None,
            'kiteworks_hostname' : None,
            'kiteworks_upload_folder' : None,
            'archive_root_directory' : None,
            'ezdb_root_directory' : None,
            'downloads_internal_directory' : None,
            'downloads_external_directory' : None,
            'ra_monthly_filing_filename' : None,
            'month_ahead_filename' : None,
            'cam_rmr_filename' : None,
            'year_ahead_filename' : None,
            'cam_rmr_update_filename' : None,
            'incremental_local_filename' : None,
            'supply_plan_system_filename' : None,
            'supply_plan_flexible_filename' : None,
            'nqc_list_filename' : None,
            'ra_summary_filename' : None,
            'ra_summary_template_filename' : None,
            'caiso_cross_check_filename' : None,
            'caiso_cross_check_template_filename' : None,
            'results_archive_filename' : None,
            'ezdb_data_sources_filename' : None,
            'ezdb_organizations_filename' : None,
            'ezdb_requirements_filename' : None,
            'ezdb_resources_filename' : None,
            'ezdb_summaries_filename' : None,
            'ezdb_supply_plans_filename' : None,
            'ezdb_master_lookup_filename' : None,
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
            'files_for_archive' : [],
            'version_controlled_files' : [],
        }
        self.load_configuration_options(configuration_options_path)
        if filing_month is None:
            self.filing_month = self.get_option('filing_month')
        else:
            self.filing_month = filing_month
            self.options['filing_month'] = filing_month
        self.paths = Paths(self)
        self.organizations = Organizations(self.paths.get_path('organizations'))
    def load_configuration_options(self,configuration_options_path:Path):
        '''
        reads configuration file and applies options.

        parameters:
            configuration_options_path - path object linking to a yaml file
                containing configuration options for the ra_consolidator class.
        '''
        if configuration_options_path.is_file():
            self.configuration_options_path = configuration_options_path
            with self.configuration_options_path.open(mode='r') as f:
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
            self.configuration_options_path = None
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
    def get_filing_due_date(self,filing_month:td=None):
        '''
        calculates the due date for a given filing month, defaulting to the
        filing month defined for the class instance.

        parameters:
            filing_month - a pandas timestamp object representing a day within
                the filing month
        '''
        if filing_month is None:
            filing_month = self.filing_month
        else:
            pass
        filing_due_date = filing_month.replace(day=1) + td(days=-45)
        holidays = california_state_holidays(filing_month.year)
        while filing_due_date in holidays.values() or filing_due_date.weekday()>=5:
            filing_due_date += td(days=1)
        filing_due_date = filing_due_date.replace(hour=23,minute=59,second=59)
        return filing_due_date

class Paths:
    '''
    a class to handle paths and path template strings defined in the
    configuration options yaml file
    '''
    def __init__(self,config:ConfigurationOptions):
        '''
        populates the paths instance variable with parsed path objects pointing
        to each file, based on the configuration options instance variable.

        parameters:
            config - an instance of the ConfigurationOptions class
        '''
        self.filing_month = config.filing_month
        self.path_strings = {
            'configuration_options' : str(config.configuration_options_path),
            'archive_root' : config.get_option('archive_root_directory'),
            'organizations' : config.get_option('organizations_filename'),
            'email_filter' : config.get_option('email_filter_filename'),
            'downloads_internal' : config.get_option('downloads_internal_directory'),
            'downloads_external' : config.get_option('downloads_external_directory'),
            'ra_monthly_filing' : config.get_option('ra_monthly_filing_filename'),
            'month_ahead' : config.get_option('month_ahead_filename'),
            'cam_rmr' : config.get_option('cam_rmr_filename'),
            'year_ahead' : config.get_option('year_ahead_filename'),
            'cam_rmr_update' : config.get_option('cam_rmr_update_filename'),
            'incremental_local' : config.get_option('incremental_local_filename'),
            'supply_plan_system' : config.get_option('supply_plan_system_filename'),
            'supply_plan_flexible' : config.get_option('supply_plan_flexible_filename'),
            'nqc_list' : config.get_option('nqc_list_filename'),
            'ra_summary' : config.get_option('ra_summary_filename'),
            'ra_summary_template' : config.get_option('ra_summary_template_filename'),
            'caiso_cross_check' : config.get_option('caiso_cross_check_filename'),
            'caiso_cross_check_template' : config.get_option('caiso_cross_check_template_filename'),
            'results_archive' : config.get_option('results_archive_filename'),
            'ezdb_root' : config.get_option('ezdb_root_directory'),
            'ezdb_data_sources' : config.get_option('ezdb_data_sources_filename'),
            'ezdb_organizations' : config.get_option('ezdb_organizations_filename'),
            'ezdb_requirements' : config.get_option('ezdb_requirements_filename'),
            'ezdb_resources' : config.get_option('ezdb_resources_filename'),
            'ezdb_summaries' : config.get_option('ezdb_summaries_filename'),
            'ezdb_supply_plans' : config.get_option('ezdb_supply_plans_filename'),
            'ezdb_master_lookup' : config.get_option('ezdb_master_lookup_filename'),
            'webdrivers' : config.get_option('webdriver_directory'),
            'log' : config.get_option('log_filename'),
            'email_log' : config.get_option('email_log_filename'),
            'attachment_log' : config.get_option('attachment_log_filename'),
            'consolidation_log' : config.get_option('consolidation_log_filename'),
        }
        self.files_for_archive = config.get_option('files_for_archive')
        self.version_controlled_files = config.get_option('version_controlled_files')
    def parse_filename(self,filename:str,relative_root:Path,organization:dict=None,date:ts=None,version:int=0):
        '''
        parses a filename string and resolves tokens with replacement values
        based on input parameters. returns path objects defined relative to the
        current working directory.

        parameters:
            filename - a string to be resolved into a path after replacing any
                tokens with values based on other parameters
            relative_root - a Path object pointing to a directory containing the
                resolved filename
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
            '[version]' : f'{version:02.0f}',
        }
        for token in re.findall(r'\[\w[_A-Za-z]*\]',filename):
            if token in replacements.keys():
                parsed_filename = parsed_filename.replace(token,replacements[token])
            else:
                pass
        return Path(parsed_filename).relative_to(relative_root)
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
        if 'ezdb' in path_id:
            relative_root = Path(self.path_strings['ezdb_root'])
        else:
            relative_root = Path(self.path_strings['archive_root'])
        if path_id in self.path_strings.keys():
            if path_id=='ra_monthly_filing':
                if organization is None:
                    path = None
                elif version is None:
                    path = self.most_recent_version(path_id,organization=organization,date=date)
                else:
                    path = self.parse_filename(self.path_strings[path_id],relative_root,organization=organization,date=date,version=version)
            elif path_id in self.version_controlled_files:
                if version is None:
                    path = self.most_recent_version(path_id,date=date)
                else:
                    path = self.parse_filename(self.path_strings[path_id],relative_root,date=date,version=version)
            else:
                path = self.parse_filename(self.path_strings[path_id],relative_root,date=date)
            if path_id not in self.files_for_archive:
                path = relative_root / path
            else:
                pass
        elif path_id=='ra_summary_previous_month':
            previous_date = date.replace(year=date.year-int((13-date.month)/12),month=(date.month+10)%12+1)
            path = self.parse_filename(self.path_strings['ra_summary'],relative_root,date=previous_date)
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
        if 'ezdb' in path_id:
            relative_root = Path(self.path_strings['ezdb_root'])
        else:
            relative_root = Path(self.path_strings['archive_root'])
        if path_id in self.version_controlled_files:
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename.replace('[version]','[_version_]'),relative_root,organization,date)
            versions = glob.glob(str(path).replace('[_version_]','[0-9][0-9]'))
            versions.sort(reverse=True)
            if len(versions)>0:
                path = Path(versions[0])
            else:
                path = None
        elif path_id in self.path_strings.keys():
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename,relative_root,organization,date)
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
        if 'ezdb' in path_id:
            relative_root = Path(self.path_strings['ezdb_root'])
        else:
            relative_root = Path(self.path_strings['archive_root'])
        if path_id in self.version_controlled_files:
            filename = self.path_strings[path_id]
            path = self.parse_filename(filename.replace('[version]','[_version_]'),relative_root,organization,date)
            versions = glob.glob(str(path).replace('[_version_]','[0-9][0-9]'))
            versions.sort(reverse=True)
            paths = [Path(version) for version in versions]
        elif path_id in self.path_strings.keys():
            paths = [self.get_path(path_id)]
        else:
            paths = None
        return paths
    def get_version_number(self,path:Path,path_id:str,organization:dict=None,date:ts=None):
        '''
        extracts the version number from a path string according to the filename
        template set in the ConfigurationOptions object.

        parameters:
            path_id - the key for a version-controlled category of files.
            path - a path pointing to a particular version of a file of the
                category specified in the path_id
        '''
        if 'ezdb' in path_id:
            relative_root = Path(self.path_strings['ezdb_root'])
        else:
            relative_root = Path(self.path_strings['archive_root'])
        if date is None:
            date = self.filing_month
        deversioned_path_string = self.path_strings[path_id].replace('[version]','[_version_]')
        deversioned_path = self.parse_filename(deversioned_path_string,relative_root,organization,date)
        version = re.match(str(deversioned_path).replace('\\','\\\\').replace('[_version_]','(\d{2})'),str(path)).groups()[0]
        return int(version)
    def paths_for_archive(self):
        '''
        returns a list of path objects pointing to files which are to be
        archived.
        '''
        return [path for paths in [self.get_all_versions(path_id) for path_id in self.files_for_archive] for path in paths]

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
        if isinstance(alias,str) and alias.lower() in [value.lower() for values in self.alias_map.values() for value in values]:
            filter_function = lambda id: alias.lower() in [mapped_alias.lower() for mapped_alias in self.alias_map[id]]
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
            organization = {'id':'','name':'','aliases':[]}
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