import io
import os
import re
import xlrd
import shutil
import itertools
import pandas as pd
from pathlib import Path
from zipfile import ZipFile
from openpyxl import load_workbook
from datetime import datetime as dt

from configuration_options import ConfigurationOptions,Paths,Organizations
from data_extraction import open_workbook,get_data_range
from ra_logging import TextLogger,DataLogger

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

class Organizer:
    '''
    a class to open excel files within a specified directory and copy files
    recognized as Resource Adequacy Monthly Filings to a separate directory with
    a new name according to the filing contents.
    '''
    def __init__(self,configuration_path:Path):
        '''
        initializes an object of class ra_filing_organizer.
        parameters:
            configuration_path - path object pointing to a yaml file containing
                configuration options
        '''
        self.configuration_options = ConfigurationOptions(configuration_path)
        self.paths = Paths(self.configuration_options)
        self.organizations = Organizations(self.paths.get_path('organizations'))
        self.logger = TextLogger(
            self.configuration_options.get_option('cli_logging_criticalities'),
            self.configuration_options.get_option('file_logging_criticalities'),
            self.paths.get_path('log')
        )
        email_log_columns = ['email_id','sender','subject','receipt_date','included','group']
        self.email_logger = DataLogger(columns=email_log_columns,log_path=self.paths.get_path('email_log'),delimiter=',')
        attachment_log_columns = ['email_id','attachment_id','download_path','ra_category','effective_date','organization_id','archive_path']
        self.attachment_logger = DataLogger(columns=attachment_log_columns,log_path=self.paths.get_path('attachment_log'),delimiter=',')
        consolidation_log_columns = ['filing_month','ra_category','effective_date','organization_id','attachment_id','archive_path','status']
        self.consolidation_logger = DataLogger(columns=consolidation_log_columns,log_path=self.paths.get_path('consolidation_log'),delimiter=',')

    def validate_attachment(self,attachment_id:str):
        '''
        reviews an attachment linked by the input path object and categorizes
        it based on its relevance to the resource adequacy filing program.

        parameters:
            attachment_id - a string representing a single email attachment
                downloaded from kiteworks
        '''
        def set_attachment_value(column,value):
            attachment_selection = (self.attachment_logger.data.loc[:,'attachment_id']==attachment_id)
            self.attachment_logger.data.loc[attachment_selection,column] = value
        attachment = self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'attachment_id']==attachment_id),:].iloc[0]
        download_path = Path(attachment.loc['download_path'])
        emails = self.email_logger.data
        email_information = emails.loc[(emails.loc[:,'email_id']==attachment.loc['email_id']),:].iloc[0]
        log_str = 'Validating \'{}\''
        self.logger.log(log_str.format(attachment.loc['download_path']),'INFORMATION')
        if download_path.is_file():
            if download_path.suffix=='.xlsx':
                try:
                    self.logger.log('Validating Attachment: {} ({})'.format(attachment_id,download_path),'INFORMATION')
                    with open(download_path,'rb') as f:
                        in_mem_file = io.BytesIO(f.read())
                    wb = load_workbook(in_mem_file,read_only=True)
                    # monthly filing:
                    if 'Certification' in wb.sheetnames:
                        set_attachment_value('ra_category','ra_monthly_filing')
                        sheet = wb['Certification']
                        if sheet['B5'].value in self.organizations.list_all_aliases():
                            submittal_information = {
                                'date' : sheet['B3'].value,
                                'organization_full' : sheet['B5'].value,
                                'organization_id' : self.organizations.lookup_id(sheet['B5'].value),
                                'organization_representative' : {
                                    'name' : sheet['B21'].value,
                                    'title' : sheet['B23'].value,
                                    'email' : sheet['B22'].value,
                                    'sign_date' : sheet['B24'].value,
                                },
                                'organization_contact' : {
                                    'name' : sheet['B28'].value,
                                    'title' : sheet['B29'].value,
                                    'address' : '{}\n{}\n{}, {} {}'.format(sheet['B30'].value,sheet['B31'].value,sheet['B32'].value,sheet['B33'].value,sheet['B34'].value),
                                    'phone' : sheet['B35'].value,
                                    'email' : sheet['B36'].value,
                                },
                                'organization_backup_contact' : {
                                    'name' : sheet['B40'].value,
                                    'title' : sheet['B41'].value,
                                    'phone' : sheet['B42'].value,
                                    'email' : sheet['B43'].value,
                                },
                                'compliance_period' : sheet['B3'].value,
                                'submittal_date' : sheet['B7'].value,
                            }
                            effective_date = self.configuration_options.get_option('filing_month')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                            set_attachment_value('organization_id',submittal_information['organization_id'])
                        else:
                            self.logger.log('Load Serving Entity Alias Not Recognized: \'{}\'\tAdd ID and Aliases to {}'.format(sheet['B5'].value,self.paths.get_path('organizations')),'WARNING')
                            effective_date = self.configuration_options.get_option('filing_month')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                            set_attachment_value('organization_id','[LSE Alias Not Recognized]')
                    # incremental local:
                    elif 'IncrementalLocal' in wb.sheetnames and email_information.loc['group']=='internal':
                        set_attachment_value('ra_category','incremental_local')
                        set_attachment_value('organization_id','CEC')
                        if re.match(r'.*(\d{4}).*',download_path.name):
                            effective_date = dt.strptime(re.match(r'.*(\d{4}).*',download_path.name).groups()[0],'%Y')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                        else:
                            effective_date = self.configuration_options.get_option('filing_month').replace(month=7,day=1)
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d %H:%M:%S'))
                    # year ahead:
                    elif 'loadforecastinputdata' in wb.sheetnames and email_information.loc['group']=='internal':
                        set_attachment_value('ra_category','year_ahead')
                        set_attachment_value('organization_id','CEC')
                        if re.match(r'.*(\d{4}).*',download_path.name):
                            effective_date = dt.strptime(re.match(r'.*(\d{4}).*',download_path.name).groups()[0],'%Y')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                        else:
                            effective_date = self.configuration_options.get_option('filing_month')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                    # month ahead:
                    elif 'Monthly Tracking' in wb.sheetnames and email_information.loc['group']=='internal':
                        set_attachment_value('ra_category','month_ahead')
                        set_attachment_value('organization_id','CPUC')
                        if re.match(r'.*(\d{4}).*',download_path.name):
                            effective_date = dt.strptime(re.match(r'.*(\d{4}).*',download_path.name).groups()[0],'%Y')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                        else:
                            effective_date = self.configuration_options.get_option('filing_month')
                            set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                    # cam-rmr:
                    elif 'CAMRMR' in wb.sheetnames and email_information.loc['group']=='internal':
                        effective_date = dt.strptime(wb['CAMRMR']['A1'].value,'%bMA%y')
                        set_attachment_value('ra_category','cam_rmr')
                        set_attachment_value('organization_id','CPUC')
                        set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                    else:
                        set_attachment_value('ra_category','none')
                        set_attachment_value('organization_id','n/a')
                        set_attachment_value('effective_date','n/a')
                    wb.close()
                except:
                    self.logger.log('Input Excel File Does Not Match Templates: '.format(download_path),'INFORMATION')
                    set_attachment_value('ra_category','none')
                    set_attachment_value('effective_date','n/a')
                    set_attachment_value('organization_id','n/a')
            elif download_path.suffix=='.xls' and email_information.loc['group']=='internal':
                wb = xlrd.open_workbook(download_path)
                sheet = wb.sheet_by_index(0)
                columns = [sheet.cell_value(rowx=0,colx=column_number) for column_number in range(sheet.ncols)]
                system_columns = [
                    r'\s*validation\s*',
                    r'\s*scid\s*',
                    r'.*id\s*',
                    r'\s*local.*',
                    r'\s*system.*',
                    r'.*total.*',
                    r'.*start.*',
                    r'.*end.*',
                    r'.*lse.*',
                    r'\s*errors.*warnings\s*',
                ]
                local_columns = [
                    r'\s*validation\s*',
                    r'\s*supplier\s*',
                    r'.*id\s*',
                    r'\s*category\s*',
                    r'\s*flex.*',
                    r'.*start.*',
                    r'.*end.*',
                    r'\s*lse\s*',
                    r'\s*errors.*warnings\s*',
                ]
                if len(columns)>=len(system_columns) and all([re.match(s,columns[i].lower()) for i,s in enumerate(system_columns)]):
                    effective_date = dt.strptime(re.match(r'.*(\d{1,2}_\d{1,2}_\d{2}).*',download_path.name).groups()[0],'%m_%d_%y')
                    effective_date = effective_date.replace(effective_date.year+int((effective_date.month+1)/12),(effective_date.month+1)%12+1,1)
                    set_attachment_value('ra_category','supply_plan_system')
                    set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                    set_attachment_value('organization_id','CAISO')
                elif len(columns)>=len(local_columns) and all([re.match(s,columns[i].lower()) for i,s in enumerate(local_columns)]):
                    effective_date = dt.strptime(re.match(r'.*(\d{1,2}_\d{2}_\d{2}).*',download_path.name).groups()[0],'%m_%d_%y')
                    effective_date = effective_date.replace(effective_date.year+int((effective_date.month+1)/12),(effective_date.month+1)%12+1,1)
                    set_attachment_value('ra_category','supply_plan_local')
                    set_attachment_value('effective_date',effective_date.strftime('%Y-%m-%d'))
                    set_attachment_value('organization_id','CAISO')
                else:
                    self.logger.log('Input Excel File Does Not Match Templates: '.format(download_path),'INFORMATION')
                    set_attachment_value('ra_category','none')
                    set_attachment_value('effective_date','n/a')
                    set_attachment_value('organization_id','n/a')
            else:
                self.logger.log('Skipping {} File: {}'.format(download_path.suffix,download_path),'INFORMATION')
                set_attachment_value('ra_category','none')
                set_attachment_value('effective_date','n/a')
                set_attachment_value('organization_id','n/a')
        else:
            self.logger.log('Input File Not Found: {}'.format(download_path),'WARNING')
            set_attachment_value('ra_category','none')
            set_attachment_value('effective_date','n/a')
            set_attachment_value('organization_id','n/a')
        self.attachment_logger.commit()
    
    def ingest_manual_downloads(self):
        '''
        checks for files in the download directories that do not appear in the
        attachment log and adds them as manual overrides.
        '''
        time_now = dt.now()
        for path in itertools.chain(self.paths.get_path('downloads_internal').iterdir(),self.paths.get_path('downloads_external').iterdir()):
            if path.is_file() and str(path) not in self.attachment_logger.data.loc[:,'download_path'].values:
                email_id = '00000000-0000-0000-0000-0000{}'.format(time_now.strftime('%Y%m%d'))
                if email_id not in self.email_logger.data.loc[:,'email_id'].values:
                    if path.parent==self.paths.get_path('downloads_internal'):
                        sender_group = 'internal'
                    else:
                        sender_group = 'external'
                    email_information = pd.Series({
                        'email_id' : email_id,
                        'sender' : 'manual_download',
                        'subject' : 'n/a',
                        'receipt_date' : time_now.strftime('%Y-%m-%d %H:%M:%S'),
                        'included' : True,
                        'group' : sender_group,
                    })
                    self.email_logger.log(email_information)
                attachment_index = len(self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'email_id']==email_id),'attachment_id'])
                attachment_id = '{}{:024.0f}'.format(time_now.strftime('%Y%m%d'),attachment_index)
                attachment_information = pd.Series({
                    'email_id' :email_id,
                    'attachment_id' : attachment_id,
                    'download_path' : str(path),
                    'ra_category' : 'not_validated',
                    'effective_date' : '',
                    'archive_path' : '',
                })
                self.attachment_logger.log(attachment_information)
        self.attachment_logger.commit()
        self.email_logger.commit()

    def validate_all(self):
        '''
        validates each entry in the attachments_log and fills additional
        information based on attachment contents.
        '''
        for attachment in self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'ra_category']=='not_validated'),:].iterrows():
            attachment_id = attachment[1].loc['attachment_id']
            self.validate_attachment(attachment_id)

    def set_versions(self):
        '''
        determines the version numbers for each attachment in the attachment
        log and sets the archive path for all entries in the attachment_log
        which don't yet have one.
        '''
        columns = self.attachment_logger.data.columns
        attachments = self.attachment_logger.data.merge(self.email_logger.data.loc[:,['email_id','receipt_date']],on='email_id').fillna('n/a')
        attachments.loc[:,'version'] = \
            attachments.sort_values(['ra_category','organization_id','effective_date','receipt_date']) \
            .groupby(['ra_category','organization_id','effective_date']).cumcount()
        def get_archive_path(r):
            ra_category = r.loc['ra_category']
            organization = self.organizations.get_organization(r.loc['organization_id'])
            if r.loc['effective_date'] not in ('','n/a'):
                effective_date = dt.strptime(r.loc['effective_date'],'%Y-%m-%d')
            else:
                effective_date = self.configuration_options.get_option('filing_month')
            version = r.loc['version']
            archive_path = str(self.paths.get_path(ra_category,organization=organization,date=effective_date,version=version))
            return archive_path
        self.attachment_logger.data.loc[:,'archive_path'] = attachments.apply(get_archive_path,axis='columns')
        self.attachment_logger.data = self.attachment_logger.data.loc[:,columns]
        self.attachment_logger.commit()
    
    def copy_rename(self,attachment_id:str):
        '''
        copies a single attachment identified by its attachment_id to the archive path if the file doesn't already exist.

        parameters:
            attachment_id - a string representing a single email attachment
                downloaded from kiteworks
        '''
        attachment = self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'attachment_id']==attachment_id),:].iloc[0]
        download_path = attachment.loc['download_path']
        archive_path = attachment.loc['archive_path']
        if type(archive_path)==str:
            if Path(download_path).is_file() and not Path(archive_path).is_file():
                Path(archive_path).parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(download_path,archive_path)
                self.logger.log('Copying {} to {}'.format(download_path,archive_path),'INFORMATION')
            elif Path(archive_path).is_file():
                self.logger.log('Skipping File {} -- Already Exists in Archive as {}'.format(download_path,archive_path),'INFORMATION')
            else:
                self.logger.log('Unable to Copy {} to Archive'.format(download_path),'INFORMATION')
        else:
            self.logger.log('Skipping File {} -- Does Not Match Any RA Templates'.format(download_path),'INFORMATION')
    
    def copy_rename_all(self):
        '''
        copies all downloaded attachments to their archive locations
        '''
        for attachment_id in self.attachment_logger.data.loc[:,'attachment_id']:
            self.copy_rename(attachment_id)

    def unzip(self,path:Path):
        '''
        expands a single zip archive into its parent directory.

        parameters:
        path - 
        '''
        if path.is_file():
            try:
                with ZipFile(str(path),'r') as z:
                    z.extractall(path.parent)
                self.logger.log('Decompressing {} Archive: {}'.format(path.suffix,path),'INFORMATION')
            except:
                self.logger.log('Unable to Decompress Archive','WARNING')
        else:
            self.logger.log('File Not Found: {}'.format(path),'WARNING')

    # traverse directory tree, opening sub-directories recursively and copying/renaming files:
    def traverse(self,directory_path: Path,download_source: str):
        '''
        loops through contents of a directory, copying files to their archive
        directory based on the download source and recursively opening
        sub-directories.

        parameters:
            directory_path - path object pointing to the directory to traverse
            download_source - string indicating whether to treat directory
                contents as internal or external
        '''
        if directory_path.is_dir():
            for item in directory_path.iterdir():
                if item.is_dir():
                    self.traverse(item,download_source)
                elif item.is_file():
                    self.copy_rename(item,download_source)
                else:
                    self.logger.log('Unknown Item: {}'.format(item),'WARNING')

    def cleanup(self,directory_path: Path):
        '''
        deletes all contents of the specified directory.

        parameters:
            directory_path - path object pointing to the directory to clean up
        '''
        if directory_path.is_dir():
            for item in directory_path.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                elif item.is_file():
                    os.remove(item)
        elif directory_path.is_file():
            os.remove(directory_path)
        else:
            pass
    
    def check_files(self):
        '''
        Checks whether all files required for consolidation are available and provides a table of results

        returns:
            boolean - true if all files are available, false otherwise
            dataframe - contains table of files and statuses
        '''
        def set_file_status(ra_category,organization_id):
            attachments = self.attachment_logger.data
            filing_month = self.configuration_options.get_option('filing_month')
            if ra_category in ('ra_monthly_filing','supply_plan_system','supply_plan_local'):
                effective_date = filing_month
            else:
                effective_date = filing_month.replace(month=1)
            versions = attachments.loc[
                (attachments.loc[:,'ra_category']==ra_category) & \
                (attachments.loc[:,'effective_date']==effective_date.strftime('%Y-%m-%d')) & \
                (attachments.loc[:,'organization_id']==organization_id), \
                ['attachment_id','archive_path']
            ]
            versions.sort_values('archive_path',ascending=False,inplace=True)
            file_information = pd.Series({
                'filing_month' : filing_month.strftime('%Y-%m-%d'),
                'ra_category' : ra_category,
                'effective_date' : effective_date.strftime('%Y-%m-%d'),
                'organization_id' : organization_id,
                'attachment_id' : '',
                'archive_path' : '',
                'status' : '',
            })
            if len(versions)>0 and Path(versions.iloc[0].loc['archive_path']).is_file():
                file_information.loc['attachment_id'] = versions.iloc[0].loc['attachment_id']
                file_information.loc['archive_path'] = versions.iloc[0].loc['archive_path']
                file_information.loc['status'] = 'Ready'
            elif ra_category=='incremental_local' and filing_month.month<=6:
                file_information.loc['attachment_id'] = ''
                file_information.loc['archive_path'] = ''
                file_information.loc['status'] = 'Not Required'
            elif ra_category=='ra_monthly_filing':
                if len(versions)>0:
                    file_information.loc['attachment_id'] = versions.iloc[0].loc['attachment_id']
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'Invalid File'
                else:
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'File Not Submitted'
            elif self.paths.get_path(ra_category) is not None:
                if self.paths.get_path(ra_category).is_file():
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = str(self.paths.get_path(ra_category))
                    file_information.loc['status'] = 'Ready'
                else:
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'File Not Found'
            else:
                file_information.loc['attachment_id'] = ''
                file_information.loc['archive_path'] = ''
                file_information.loc['status'] = 'File Not Found'
            # overwrite previous entries for a given file:
            check_columns = ['filing_month','ra_category','effective_date','organization_id']
            previous_log_indices = (self.consolidation_logger.data.loc[:,check_columns]==file_information.loc[check_columns]).apply(all,axis='columns',result_type='reduce')
            if previous_log_indices.sum()>0:
                self.consolidation_logger.data.drop(index=self.consolidation_logger.data.loc[previous_log_indices,:].index,inplace=True)
            else:
                pass
            self.consolidation_logger.log(file_information)
        # get list of current lses from summary template file:
        path = self.paths.get_path('ra_summary_template')
        ra_summary = open_workbook(path,data_only=True,read_only=True)
        data_range = get_data_range(ra_summary['Summary'],'A','',self.organizations)
        active_lses = [row[0].value for row in data_range]
        ra_categories = ['year_ahead','incremental_local','month_ahead','cam_rmr','supply_plan_system','supply_plan_local'] + ['ra_monthly_filing'] * len(active_lses)
        organization_ids = ['CEC','CEC','CPUC','CPUC','CAISO','CAISO'] + active_lses
        # get list of active load serving entities from summary sheet:
        for ra_category,organization_id in zip(ra_categories,organization_ids):
            set_file_status(ra_category,organization_id)
        ready =  all([s in ('Ready','Not Required') for s in self.consolidation_logger.data.loc[(self.consolidation_logger.data.loc[:,'ra_category']!='ra_monthly_filing'),'status']])
        self.consolidation_logger.commit()
        return ready
    
    def organize(self):
        '''
        calls each of the main methods of the ra_organizer method in sequence
        to handle downloaded and manually placed input files and prepare for
        consolidation and summarization.
        '''
        # for item in itertools.chain(self.paths.get_path('downloads_internal').iterdir(),self.paths.get_path('downloads_external').iterdir()):
        #     if item.suffix=='.zip':
        #         self.unzip(item)
        #     else:
        #         pass
        # self.traverse(self.paths.get_path('downloads_internal'),'internal')
        # self.traverse(self.paths.get_path('downloads_external'),'external')
        # self.cleanup(self.paths.get_path('download_directory'))
        self.ingest_manual_downloads()
        self.validate_all()
        self.set_versions()
        self.copy_rename_all()

    def compress_archive(self):
        # get list of current lses from summary template file:
        path = self.paths.get_path('ra_summary_template')
        ra_summary = open_workbook(path,data_only=True,read_only=True)
        data_range = get_data_range(ra_summary['Summary'],'A','',self.organizations)
        active_lses = [row[0].value for row in data_range]
        path_ids = [
            ('year_ahead','CEC'),
            ('incremental_local','CEC'),
            ('month_ahead','CPUC'),
            ('cam_rmr','CPUC'),
            ('supply_plan_system','CAISO'),
            ('supply_plan_local','CAISO'),
            ('ra_summary','CPUC'),
            ('caiso_cross_check','CPUC'),
            ('log','CPUC'),
            ('email_log','CPUC'),
            ('attachment_log','CPUC'),
            ('consolidation_log','CPUC'),
        ] + list(zip(['ra_monthly_filing'] * len(active_lses),active_lses))
        paths = [self.paths.get_path(path_id[0],organization=self.organizations.get_organization(path_id[1])) for path_id in path_ids] 
        if self.paths.get_path('results_archive').exists():
            self.paths.get_path('results_archive').unlink()
        else:
            pass
        with ZipFile(self.paths.get_path('results_archive'),'x') as archive:
            for path in paths:
                if path is not None and path.is_file():
                    archive.write(path,arcname=str(path))
                else:
                    pass