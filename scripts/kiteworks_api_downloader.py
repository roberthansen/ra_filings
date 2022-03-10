import re
import pandas as pd
from pathlib import Path
from yaml import safe_load
from itertools import chain
from functools import reduce
from datetime import datetime as dt

from ra_logging import TextLogger,DataLogger
from kiteworks_api import KiteworksAPI
from configuration_options import ConfigurationOptions,Paths,EmailFilter

class AttachmentDownloader:
    '''
    class for downloading attachments from multiple kiteworks emails
    '''
    def __init__(self,configuration_path:Path,user:dict,api_client:dict,connection:KiteworksAPI=None):
        '''
        initializes the attachment_downloader class and sets up a connection to
        the kiteworks api
        parameters:
            configuration_path - path pointing to the resource adequacy
                configruation options yaml file
            user - a dictionary containing kiteworks user information
            api_client - a dictionary containing kiteworks api client
                information, including access credentials
            connection - an optional instance of the KiteworksAPI class; if not
                included, one is created
        '''
        self.configuration_options = ConfigurationOptions(configuration_path)
        self.paths = Paths(self.configuration_options)
        self.email_filter = EmailFilter(self.paths.get_path('email_filter'))
        self.logger = TextLogger(
            self.configuration_options.get_option('cli_logging_criticalities'),
            self.configuration_options.get_option('file_logging_criticalities'),
            self.paths.get_path('log')
        )
        email_log_columns = ['email_id','sender','subject','receipt_date','included','group']
        self.email_logger = DataLogger(
            columns=email_log_columns,
            log_path=self.paths.get_path('email_log'),
            delimiter=','
        )
        attachment_log_columns = ['email_id','attachment_id','download_path','ra_category','effective_date','organization_id','archive_path']
        self.attachment_logger = DataLogger(
            columns=attachment_log_columns,
            log_path=self.paths.get_path('attachment_log'),
            delimiter=','
        )
        if connection is None:
            kiteworks_hostname = self.configuration_options.get_option('kiteworks_hostname')
            client_app_id = api_client['app_id']
            client_app_key = api_client['app_key']
            signature = api_client['signature']
            upload_folder = self.configuration_options.get_option('kiteworks_upload_folder')
            user_id = user['uid']
            api_scope = 'GET/clients/* PUT/clients/* */fileTypes/* POST/files/* GET/folders/* GET/files/* PUT/files/* PATCH/files/* GET/mail/* */mediaTypes/* GET/notifications/* GET/uploads/*'
            redirect_uri = kiteworks_hostname + '/rest/callback.html'
            access_token_endpoint =  kiteworks_hostname + '/oauth/token'
            self.connection = KiteworksAPI(kiteworks_hostname,client_app_id,client_app_key,signature,user_id,api_scope,redirect_uri,access_token_endpoint,upload_folder)
        else:
            self.connection = connection

    def download_filtered(self,start_date,end_date):
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
        response = self.connection.list_email_in_date_range(start_date,end_date)
        email_id_list = [x['id'] for x in response.json()['data']]
        internal_address_check = re.compile(r'\S*@cpuc\.ca\.gov$')
        file_type_check = re.compile(r'\S*\.(xlsx|xls)$')
        log_str = 'Searching for Emails from {} to {}'
        self.logger.log(log_str.format(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d')),'INFORMATION')
        for email_id in email_id_list:
            response = self.connection.list_attachments(email_id)
            attachment_list = response.json()['data']
            response = self.connection.get_message(email_id)
            email = response.json()
            email_date = dt.strptime(email['date'],'%Y-%m-%dT%H:%M:%S%z')
            email_sender = email['emailReturnReceipt'][0]['user']['email']
            email_subject = email['subject']
            self.paths.get_path('downloads_external').mkdir(parents=True,exist_ok=True)
            self.paths.get_path('downloads_internal').mkdir(parents=True,exist_ok=True)
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
                'receipt_date' : email_date.strftime('%Y-%m-%d %H:%M:%S'),
                'included' : str(include_email),
                'group' : sender_group,
            })
            if email_id not in self.email_logger.data.loc[:,'email_id']:
                self.email_logger.log(email_information)
            else:
                pass
            if include_email:
                for attachment in filter(lambda a: file_type_check.match(a['name']),attachment_list):
                    if attachment['attachmentId'] not in self.attachment_logger.data.loc[:,'attachment_id'] and attachment['name'] not in [p.name for p in chain(self.paths.get_path('downloads_internal').iterdir(),self.paths.get_path('downloads_external').iterdir())]:
                        log_str = 'Downloading Attachment \'{}\' --- Date: {}; Subject: {}; Sender: {}'
                        self.logger.log(log_str.format(attachment['name'],email_date.strftime('%Y-%m-%d'),email_subject,email_sender),'INFORMATION')
                        if internal_address_check.match(email_sender):
                            download_path = self.paths.get_path('downloads_internal') / attachment['name']
                        else:
                            download_path = self.paths.get_path('downloads_external') / attachment['name']
                        self.connection.download_attachment(email_id,attachment['attachmentId'],download_path)
                        attachment_information = pd.Series({
                            'email_id' : email_id,
                            'attachment_id' : attachment['attachmentId'],
                            'download_path' : str(download_path),
                            'ra_category' : 'not_validated',
                            'effective_date' : '',
                            'organization_id' : '',
                            'archive_path' : '',
                        })
                        self.attachment_logger.log(attachment_information)
                    else:
                        log_str = 'Attachment \'{}\' Already Downloaded'
                        self.logger.log(log_str.format(attachment['name']))
        self.email_logger.commit()
        self.attachment_logger.commit()
    def download_filing_month(self):
        '''
        downloads attachments from emails created in a pre-set date range of
        the 10th through the 20th of the month two months prior to a given
        filing month
        '''
        filing_month = self.configuration_options.get_option('filing_month')
        start_date = dt(year=filing_month.year+int((filing_month.month+10)/12)-1,month=(filing_month.month+9)%12+1,day=1)
        end_date = start_date.replace(day=28)
        self.download_filtered(start_date,end_date)
    def send_results(self):
        '''
        checks whether a zip archive exists for the filing month set in the
        configuration options and sends the archive via email to the resource
        adequacy team.
        '''
        filing_month = self.configuration_options.get_option('filing_month')
        invalid_filings = []
        if len(invalid_filings)>0:
            invalid_filings_str = '<p>The following monthly filings were not included in the summary and cross-check workbooks due to validation errors:</p><ul>{}</ul><p>Please review the filings and either send repaired files to rafiling@cpuc.ca.gov or ask the load-serving entity to re-submit.</p>'.format(''.join(['<li>'+invalid_filing+'</li>' for invalid_filing in invalid_filings]))
        else:
            invalid_filings_str = ''
        message = {
            'to' : ['rafiling@cpuc.ca.gov'],
            'cc' : [],
            'bcc' : ['rh2@cpuc.ca.gov'],
            'subject' : 'Resource Adequacy Filing Results for {}'.format(filing_month.strftime('%B, %Y')),
            'body' : '<p>Hello Resource Adequacy Team,</p><p><br></p><p>Please find the summary and supply plan cross-check workbooks pertaining to the Resource Adequacy Monthly Filings for {} in the attached zip file. This file also includes each load-serving entity\'s filings, other input files, and logs generated while performing the summarization.</p>{}<p><br></p><p>This message was generated automatically by the ra_filing script.</p>'.format(filing_month.strftime('%B, %Y'),invalid_filings_str),
            'acl' : 'verify_recipient',
            'draft' : 0,
            'includeFingerprint' : 0,
            'isSelfReturnReceipt' : 1,
            'notifyExpired' : 0,
            'returnReceipts' : [],
            'selfCopy' : 0,
        }
        paths = [self.paths.get_path('results_archive')]
        self.connection.send_message(message,paths)