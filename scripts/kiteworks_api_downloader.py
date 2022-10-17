import re
import pandas as pd
from pathlib import Path
from itertools import chain
from pandas import Timestamp as ts,Timedelta as td

from configuration_options import ConfigurationOptions,EmailFilter
from ra_logging import TextLogger,EmailLogger,AttachmentLogger,ConsolidationLogger
from data_extraction import load_workbook,get_cross_check_tables
from kiteworks_api import KiteworksAPI

class AttachmentDownloader:
    '''
    class for downloading attachments from multiple kiteworks emails
    '''
    def __init__(self,configuration_options_path:Path,user:dict,api_client:dict,connection:KiteworksAPI=None,filing_month:ts=None):
        '''
        initializes the AttachmentDownloader class and sets up a connection to
        the kiteworks api.

        parameters:
            configuration_options_path - path pointing to the resource adequacy
                configruation options yaml file
            user - a dictionary containing kiteworks user information
            api_client - a dictionary containing kiteworks api client
                information, including access credentials
            connection - an optional instance of the KiteworksAPI class; if not
                included, one is created
        '''
        self.config = ConfigurationOptions(configuration_options_path,filing_month=filing_month)
        self.email_filter = EmailFilter(self.config.paths.get_path('email_filter'))
        self.logger = TextLogger(
            self.config.get_option('cli_logging_criticalities'),
            self.config.get_option('file_logging_criticalities'),
            self.config.paths.get_path('log')
        )
        self.email_logger = EmailLogger(log_path=self.config.paths.get_path('email_log'))
        self.attachment_logger = AttachmentLogger(log_path=self.config.paths.get_path('attachment_log'))
        self.consolidation_logger = ConsolidationLogger(log_path=self.config.paths.get_path('consolidation_log'))
        if connection is None:
            kiteworks_hostname = self.config.get_option('kiteworks_hostname')
            client_app_id = api_client['app_id']
            client_app_key = api_client['app_key']
            signature = api_client['signature']
            upload_folder = self.config.get_option('kiteworks_upload_folder')
            user_id = user['uid']
            api_scope = '*/*/*'
            redirect_uri = kiteworks_hostname + '/rest/callback.html'
            access_token_endpoint =  kiteworks_hostname + '/oauth/token'
            self.connection = KiteworksAPI(kiteworks_hostname,client_app_id,client_app_key,signature,user_id,api_scope,redirect_uri,access_token_endpoint,upload_folder)
        else:
            self.connection = connection

    def download_filtered(self,start_date:ts,end_date:ts):
        '''
        retrieves a list of emails for the inbox specified by user name when
        connecting to the kiteworks api, then loops through each email and each
        attachment within an email, downloading attachments matching a set of
        filter criteria to the internal and external download directories
        specified in the configuration options file.

        parameters:
            start_date - datetime object representing this earliest email
                creation date in a range of dates which emailswill be retrieved
            end_date - datetime object representing the latest email creation
                date in a range of dates from which emails will be retrieved
        '''
        # ensure start_date is before end_date, otherwise swap:
        if start_date>end_date:
            swap = start_date
            start_date = end_date
            end_date = swap
        else:
            pass
        if end_date>ts.now():
            response = self.connection.list_email_since_date(start_date)
        else:
            response = self.connection.list_email_in_date_range(start_date,end_date)
        email_list = response.json()['data']
        internal_address_check = re.compile(r'\S*@cpuc\.ca\.gov$')
        file_type_check = re.compile(r'.*\.(xlsx|xls)$')
        log_str = 'Searching for Emails from {} to {}'
        self.logger.log(log_str.format(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d')),'INFORMATION')
        for email in email_list:
            email_id = email['id']
            email_date = ts(email['date']).tz_convert(None)
            email_sender = email['emailReturnReceipt'][0]['user']['email']
            email_subject = email['subject']
            self.config.paths.get_path('downloads_external').mkdir(parents=True,exist_ok=True)
            self.config.paths.get_path('downloads_internal').mkdir(parents=True,exist_ok=True)
            include_email = self.email_filter.check_email_subject(email_subject)
            internal_sender = internal_address_check.match(email_sender)
            if internal_sender:
                sender_group = 'internal'
            else:
                sender_group = 'external'
            email_information = pd.Series({
                'email_id' : email_id,
                'sender' : email_sender,
                'subject' : email_subject,
                'receipt_date' : email_date,
                'included' : str(include_email),
                'group' : sender_group,
            })
            if email_id not in self.email_logger.data.loc[:,'email_id'].values:
                self.email_logger.log(email_information)
            else:
                pass
            if include_email:
                response = self.connection.list_attachments(email_id)
                attachment_list = response.json()['data']
                for attachment in filter(lambda a: file_type_check.match(a['name']),attachment_list):
                    attachment_ids = self.attachment_logger.data.loc[:,'attachment_id'].values
                    attachment_names = [p.name for p in chain(self.config.paths.get_path('downloads_internal').iterdir(),self.config.paths.get_path('downloads_external').iterdir())]
                    if attachment['attachmentId'] not in attachment_ids:
                        if internal_sender:
                            download_path = self.config.paths.get_path('downloads_internal') / attachment['name']
                        else:
                            download_path = self.config.paths.get_path('downloads_external') / attachment['name']
                        if attachment['name'] not in attachment_names:
                            log_str = 'Downloading Attachment \'{}\' --- Date: {}; Subject: {}; Sender: {}'
                            self.logger.log(log_str.format(attachment['name'],email_date.strftime('%Y-%m-%d'),email_subject,email_sender),'INFORMATION')
                            self.connection.download_attachment(email_id,attachment['attachmentId'],download_path)
                        else:
                            log_str = 'Skipping Attachment - Already Downloaded \'{}\' --- Date: {}; Subject: {}; Sender: {}'
                            self.logger.log(log_str.format(attachment['name'],email_date.strftime('%Y-%m-%d'),email_subject,email_sender),'INFORMATION')
                        attachment_information = pd.Series({
                            'email_id' : email_id,
                            'attachment_id' : attachment['attachmentId'],
                            'download_path' : str(download_path),
                            'ra_category' : 'not_validated',
                            'effective_date' : 'NaT',
                            'organization_id' : '',
                            'archive_path' : '',
                        })
                        self.attachment_logger.log(attachment_information)
                    else:
                        log_str = 'Skipping Attachment - Already Downloaded \'{}\' --- Date: {}; Subject: {}; Sender: {}'
                        self.logger.log(log_str.format(attachment['name'],email_date.strftime('%Y-%m-%d'),email_subject,email_sender),'INFORMATION')
                self.attachment_logger.commit()
            self.email_logger.commit()

    def download_filing_month(self):
        '''
        downloads attachments from emails created in a pre-set date range of
        the 10th through the 20th of the month two months prior to a given
        filing month
        '''
        filing_month = self.config.filing_month
        start_date = ts(year=filing_month.year+int((filing_month.month+10)/12)-1,month=(filing_month.month+9)%12+1,day=1)
        end_date = start_date.replace(day=28)
        self.download_filtered(start_date,end_date)

    def download_current_month(self):
        '''
        downloads attachments from emails created in the past 30 days
        '''
        start_date = ts.now().replace(hour=0,minute=0,second=0,microsecond=0) - td(days=30)
        end_date = ts.now() + td(days=1)
        self.download_filtered(start_date,end_date)

    def send_invalid_filing_notification(self,consolidation_log_entry:pd.Series):
        '''
        sends an email to an lse notifying them that their filing for the current filing month did not pass validation.
        parameters:
            consolidation_log_entry - a row from the consolidation log
        '''
        attachment_id = consolidation_log_entry.loc['attachment_id']
        if isinstance(attachment_id,str):
            self.attachment_logger.load_log()
            self.email_logger.load_log()
            attachment = self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'attachment_id']==attachment_id),:]
            source_email_id = attachment.iloc[0].loc['email_id']
            organization_id = attachment.iloc[0].loc['organization_id']
            organization = self.config.organizations.get_organization(organization_id)
            organization_name = organization['name']
            download_path = Path(attachment.iloc[0].loc['download_path'])
            filing_month = self.config.filing_month
            source_email = self.email_logger.data.loc[(self.email_logger.data.loc[:,'email_id']==source_email_id),:]
            source_email_sender = source_email.iloc[0].loc['sender']
            source_email_receipt_date = source_email.iloc[0].loc['receipt_date']
            message_body = re.sub('\s+',' ','''
                <p>Hello,</p>
                <p><nbsp></p>
                <p>The attached filing for {} ({}), which was submitted on behalf of {} ({}) on {}, and
                determined the filing is not in compliance with CPUC resource
                adequacy requirements. Please check the filing against {}'s
                responsibilities and resubmit.</p>
                <p><br></p>
                <p>This message was generated automatically. Please contact the
                Resource Adequacy Team
                (<a href='mailto:rafiling@cpuc.ca.gov'>rafiling@cpuc.ca.gov</a>)
                with any questions.</p>
                <p><nbsp></p>
                <p>Sincerely,<br>The CPUC Resource Adequacy Team</p>
            ''').strip()
            if source_email_sender==organization['default_email']:
                ccs = [self.config.organizations.get_organization('CPUC')['default_email']]
            else:
                ccs = [self.config.organizations.get_organization('CPUC')['default_email'],organization['default_email']]
            message = {
                'to' : [source_email_sender],
                'cc' : ccs,
                'bcc' : ['rh2@cpuc.ca.gov'],
                'subject' : 'Invalid RA Monthly Filing: {} - {}'.format(organization_id,filing_month.strftime('%B, %Y')),
                'body' : message_body.format(
                    filing_month.strftime('%B, %Y'),
                    download_path.name,
                    organization_name,
                    organization_id,
                    source_email_receipt_date.strftime('%B %d, %Y'),
                    organization_id,
                ),
                'acl' : 'verify_recipient',
                'draft' : 0,
                'includeFingerprint' : 0,
                'isSelfReturnReceipt' : 1,
                'notifyExpired' : 0,
                'returnReceipts' : [],
                'selfCopy' : 0,
            }
            download_paths = [download_path]
        else:
            organization = self.config.organizations.get_organization(consolidation_log_entry.loc['organization_id'])
            source_email_sender = organization['default_email']
            filing_month = self.config.filing_month
            message_body = re.sub('\s+',' ','''
                <p>Hello,</p>
                <p><br></p>
                <p>We were unable to find a filing submitted for {} for {}
                ({}). Please submit a valid monthly filing as soon as
                possible.</p>
                <p><br></p>
                <p>This message was generated automatically. Please contact the
                Resource Adequacy Team 
                (<a href='mailto:rafiling@cpuc.ca.gov'>rafiling@cpuc.ca.gov</a>)
                with any questions.</p>
                <p><br></p>
                <p>Sincerely,<br>The CPUC Resource Adequacy Team</p>
            ''').strip()
            if source_email_sender==organization['default_email']:
                ccs = [self.config.organizations.get_organization('CPUC')['default_email']]
            else:
                ccs = [self.config.organizations.get_organization('CPUC')['default_email'],organization['default_email']]
            message = {
                'to' : [source_email_sender],
                'cc' : ccs,
                'bcc' : ['rh2@cpuc.ca.gov'],
                'subject' : 'Missing RA Monthly Filing: {}\'s Filing for {}'.format(organization['id'],filing_month.strftime('%B, %Y')),
                'body' : message_body.format(
                    filing_month.strftime('%B, %Y'),
                    organization['name'],
                    organization['id'],
                ),
                'acl' : 'verify_recipient',
                'draft' : 0,
                'includeFingerprint' : 0,
                'isSelfReturnReceipt' : 1,
                'notifyExpired' : 0,
                'returnReceipts' : [],
                'selfCopy' : 0,
            }
            download_paths = []
        # --- overwrite recipients: ---
        message['to'] = ['rafiling@cpuc.ca.gov']
        message['cc'] = ['rafiling@cpuc.ca.gov']
        # --- end overwrite ---
        self.logger.log('Sending invalid filing notification to {}'.format(organization['id']),'INFORMATION')
        self.connection.send_message(message,download_paths)

    def send_noncompliant_filing_notification(self,consolidation_log_entry:pd.Series):
        '''
        sends an email to an lse notifying them that their filing for the
        current filing month was evaluated and found noncompliant.

        parameters:
            consolidation_log_entry - a row from the consolidation log
        '''
        attachment_id = consolidation_log_entry.loc['attachment_id']
        self.attachment_logger.load_log()
        self.email_logger.load_log()
        attachment = self.attachment_logger.data.loc[(self.attachment_logger.data.loc[:,'attachment_id']==attachment_id),:]
        source_email_id = attachment.iloc[0].loc['email_id']
        organization_id = attachment.iloc[0].loc['organization_id']
        organization = self.config.organizations.get_organization(organization_id)
        organization_name = organization['name']
        download_path = Path(attachment.iloc[0].loc['download_path'])
        filing_month = self.config.filing_month
        # retrieve email information from email log based on attachment id:
        source_email = self.email_logger.data.loc[(self.email_logger.data.loc[:,'email_id']==source_email_id),:]
        source_email_sender = source_email.iloc[0].loc['sender']
        source_email_receipt_date = source_email.iloc[0].loc['receipt_date']
        # retrieve summary results:
        caiso_cross_check = load_workbook(self.config.paths.get_path('caiso_cross_check'))
        system_requirements,flexibility_requirements = get_cross_check_tables(caiso_cross_check,self.config)
        requirements = system_requirements.loc[organization_id,:]
        # compose email message:
        message_body = re.sub('\s+',' ','''
            <p>Hello,</p>
            <p><br></p>
            <p>The CPUC Resource Adequacy Team has evaluated the attached filing
            for {} ({}), which was submitted on behalf of {} ({}) on {}, and
            determined the filing is not in compliance with CPUC resource
            adequacy requirements--only {:,.2f} MW of the required {:,.2f} MW
            ({:.2f}%) are reported as available for {}. Please adjust {}'s
            resource availability and resubmit the filing.</p>
            <p><br></p>
            <p>This message was generated automatically. Please contact the
            <a href='mailto:rafiling@cpuc.ca.gov'>Resource Adequacy Team</a>
            with any questions.</p>
            <p><br></p>
            <p>Sincerely,<br>The CPUC Resource Adequacy Team</p>
        ''').strip()
        if source_email_sender==organization['default_email']:
            ccs = [self.config.organizations.get_organization('CPUC')['default_email']]
        else:
            ccs = [self.config.organizations.get_organization('CPUC')['default_email'],organization['default_email']]
        message = {
            'to' : [source_email_sender],
            'cc' : ccs,
            'bcc' : ['rh2@cpuc.ca.gov'],
            'subject' : 'Noncompliant RA Monthly Filing: {} - {}'.format(organization_id,filing_month.strftime('%b %Y')),
            'body' : message_body.format(
                filing_month.strftime('%b, %Y'),
                download_path.name,
                organization_name,
                organization_id,
                source_email_receipt_date.strftime('%B %d, %Y'),
                requirements.loc['physical_resources_available']+requirements.loc['demand_response_resources_available'],
                requirements.loc['resources_required'],
                100*(requirements.loc['physical_resources_available']+requirements.loc['demand_response_resources_available'])/requirements.loc['resources_required'],
                filing_month.strftime('%B'),
                organization_id
            ),
            'acl' : 'verify_recipient',
            'draft' : 0,
            'includeFingerprint' : 0,
            'isSelfReturnReceipt' : 1,
            'notifyExpired' : 0,
            'returnReceipts' : [],
            'selfCopy' : 0,
        }
        # --- overwrite recipients: ---
        message['to'] = ['rafiling@cpuc.ca.gov']
        message['cc'] = []
        # --- end overwrite ---
        self.logger.log('Sending noncompliant filing notification to {}'.format(organization['id']),'INFORMATION')
        self.connection.send_message(message,[download_path])

    def send_results(self,completed:bool):
        '''
        checks whether a zip archive exists for the filing month set in the
        configuration options and sends the archive via email to the resource
        adequacy team.
        '''
        filing_month = self.config.filing_month
        self.consolidation_logger.load_log()
        late_filings = self.consolidation_logger.data.loc[
            (self.consolidation_logger.data.loc[:,'ra_category']=='ra_monthly_filing') & \
            (self.consolidation_logger.data.loc[:,'status']=='Late'),:
        ]
        if len(late_filings)>0:
            late_filings_str = re.sub('\s+',' ','''
                <p><br></p>
                <p>The following load-serving entit{} submitted their initial monthly filing{} for {} after the due date of {}:</p>
                <ul>{}</ul>
            ''').strip().format(
                'ies' if len(late_filings)>1 else 'y',
                's' if len(late_filings)>1 else '',
                filing_month.strftime('%B, %Y'),
                self.config.get_filing_due_date().strftime('%B %d, %Y'),
                ''.join(['<li>{} ({})</li>'.format(self.config.organizations.get_name(late_filing.loc['organization_id']),late_filing.loc['organization_id']) for _,late_filing in late_filings.iterrows()])
            )
        else:
            late_filings_str = ''
        invalid_filings = self.consolidation_logger.data.loc[
            (self.consolidation_logger.data.loc[:,'ra_category']=='ra_monthly_filing') & (
                (self.consolidation_logger.data.loc[:,'status']=='Invalid File') | \
                (self.consolidation_logger.data.loc[:,'status']=='File Not Found') | \
                (self.consolidation_logger.data.loc[:,'status']=='File Not Submitted')
            ),:
        ]
        if len(invalid_filings)>0:
            # for _,invalid_filing in invalid_filings.iterrows():
            #     self.send_invalid_filing_notification(invalid_filing)
            invalid_filings_str = re.sub('\s+',' ','''
                <p><br></p>
                <p>The monthly filing{} for the following load-serving entit{} not included in the
                summary and cross-check workbooks either because {} did
                not pass validation or {} not submitted:</p>
                <ul>{}</ul>
                <p>Please review the filing{} and either send{} repaired file{} to
                rafiling@cpuc.ca.gov or ask the load-serving entit{} to
                resubmit.</p>
            ''').strip().format(
                's' if len(invalid_filings)>1 else '',
                'ies are' if len(invalid_filings)>1 else ' is',
                'they' if len(invalid_filings)>1 else 'it',
                'were' if len(invalid_filings)>1 else 'was',
                ''.join(['<li>{} ({})</li>'.format(self.config.organizations.get_name(r.loc['organization_id']),r.loc['organization_id']) for _,r in invalid_filings.iterrows()]),
                's' if len(invalid_filings)>1 else '',
                '' if len(invalid_filings)>1 else ' a',
                's' if len(invalid_filings)>1 else '',
                'ies' if len(invalid_filings)>1 else 'y'
            )
        else:
            invalid_filings_str = ''
        if completed:
            noncompliant_filings = self.consolidation_logger.data.loc[
                (self.consolidation_logger.data.loc[:,'compliance']=='Noncompliant') & \
                (self.consolidation_logger.data.loc[:,'status']=='Ready') \
                ,:
            ]
            if len(noncompliant_filings)>0:
                for _,noncompliant_filing in noncompliant_filings.iterrows():
                    self.send_noncompliant_filing_notification(noncompliant_filing)
                noncompliant_filings_str = re.sub('\s+',' ','''
                    <p><br></p>
                    <p>The following load-serving entit{} being notified that
                    their current monthly filing{} non-compliant based on CPUC's
                    resource adequacy requirements:</p>
                    <ul>{}</ul>
                    <p>Please follow up with the LSE{} to ensure adequate resources
                    are available for {}.</p>
                ''').strip().format(
                    'ies are' if len(noncompliant_filings)>1 else 'y is',
                    's are' if len(noncompliant_filings)>1 else ' is',
                    ''.join(['<li>{} ({})</li>'.format(self.config.organizations.get_name(r.loc['organization_id']),r.loc['organization_id']) for _,r in noncompliant_filings.iterrows()]),
                    's' if len(noncompliant_filings)>1 else '',
                    filing_month.strftime('%B, %Y')
                )
            else:
                noncompliant_filings_str = ''
            message = {
                'to' : [self.config.organizations.get_organization('CPUC')['default_email']],
                'cc' : [],
                'bcc' : ['rh2@cpuc.ca.gov'],
                'subject' : 'Resource Adequacy Filing Results for {}'.format(filing_month.strftime('%B, %Y')),
                'body' : re.sub('\s+',' ','''
                    <p>Hello Resource Adequacy Team,</p>
                    <p><br></p>
                    <p>Please find the summary and supply plan cross-check
                    workbooks pertaining to the Resource Adequacy Monthly
                    Filings for {} in the attached zip file. This file also
                    includes each load-serving entity\'s filings, other input
                    files, and logs generated while performing the
                    summarization.</p> {} {} {}
                    <p><br></p>
                    <p>This message was generated automatically by the RA
                    Monthly Filing Compliance Tool.</p>
                ''').strip().format(filing_month.strftime('%B, %Y'),late_filings_str,invalid_filings_str,noncompliant_filings_str),
                'acl' : 'verify_recipient',
                'draft' : 0,
                'includeFingerprint' : 0,
                'isSelfReturnReceipt' : 1,
                'notifyExpired' : 0,
                'returnReceipts' : [],
                'selfCopy' : 0,
            }
            self.logger.log('Sending Complete Results to {}'.format(self.config.organizations.get_organization('CPUC')['default_email']),'INFORMATION')
        else:
            missing_allocations = self.consolidation_logger.data.loc[
                (self.consolidation_logger.data.loc[:,'ra_category']!='ra_monthly_filing') & \
                (self.consolidation_logger.data.loc[:,'status']=='File Not Found') \
                ,:
            ]
            ra_category_names= {
                'year_ahead' : 'Year-Ahead',
                'incremental_local' : 'Incremental Local',
                'month_ahead' : 'Month-Ahead',
                'cam_rmr' : 'CAM-RMR',
                'supply_plan_system' : 'CAISO Supply Plan - System',
                'supply_plan_flexible' : 'CAISO Supply Plan - Flexible',
            }
            message = {
                'to' : [self.config.organizations.get_organization('CPUC')['default_email']],
                'cc' : [],
                'bcc' : ['rh2@cpuc.ca.gov'],
                'subject' : 'Unable to Generate Resource Adequacy Filing Summary Results for {}'.format(filing_month.strftime('%B, %Y')),
                'body' : re.sub('\s+',' ','''
                    <p>Hello Resource Adequacy Team,</p>
                    <p><br></p>
                    <p>The RA Monthly Filing Compliance Tool was unable to
                    complete its check for {} because the following files were
                    not found:</p>
                    <ul>{}</ul>
                    <p>The attached zip file contains all available filings,
                    logs, and other files relevant to the {} compliance check.
                    Please check the formats and filenames of the missing files
                    and send them to rafiling@cpuc.ca.gov via Kiteworks.</p> {} {}
                    <p><br></p>
                    <p>This message was generated automatically by the RA
                    Monthly Filing Compliance Tool.</p>
                ''').strip().format(
                    filing_month.strftime('%B, %Y'),
                    ' '.join(['<li>'+ra_category_names[r.loc['ra_category']]+'</li>' for _,r in missing_allocations.iterrows()]),
                    filing_month.strftime('%B'),
                    late_filings,
                    invalid_filings
                ),
                'acl' : 'verify_recipient',
                'draft' : 0,
                'includeFingerprint' : 0,
                'isSelfReturnReceipt' : 1,
                'notifyExpired' : 0,
                'returnReceipts' : [],
                'selfCopy' : 0,
            }
            self.logger.log('Sending Partial Results to {}'.format(self.config.organizations.get_organization('CPUC')['default_email']),'INFORMATION')
        paths = [self.config.paths.get_path('results_archive')]
        self.connection.send_message(message,paths)
        return message