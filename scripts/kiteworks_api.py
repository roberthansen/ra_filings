import hmac
import requests
from pathlib import Path
from hashlib import sha1
from random import randrange
from datetime import datetime as dt
from base64 import b64encode as benc

class kiteworks_api_connection:
    '''
    this class establishes a connection to a kiteworks api server and includes
    methods to retrieve an authentication token via oauth2 authorization code,
    and to make a variety of api calls.
    by robert hansen for the california public utilities commission
    2022-02-16
    '''
    def __init__(self,kiteworks_hostname:str,client_app_id:str,client_app_secret_key:str,signature_key:str,user_id:str,api_scope:str,redirect_uri:str,access_token_endpoint:str,upload_folder:str):
        '''
        initializes a kiteworks api connection
        parameters:
            kiteworks_hostname - string representing the base url for the
                kiteworks system, e.g., 'https://kwftp.cpuc.ca.gov'
            client_app_id - string of 32 lowercase hexidecimal characters
                separated by four hyphens in the form
                XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
                provided by the kiteworks admin
            client_app_secret_key - string of 10 alphanumeric characters
                in the form
                XXXXXXXXXX
                provided by the kiteworks admin
            signature_key - string of 29 alphanumeric characters in the form
                XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                provided by the kiteworks admin
            user_id - string representing the name of the user account which
                will be accessed through the kiteworks api, usually in the form
                of an email address
            api_scope - string containing the api calls the connection may
                request to use, e.g., 'GET/mail/*'
            redirect_uri - string representing the url which will be sent as a
                redirect link containing the response to the request for
                an authentication token
            access_token_endpoint - string representing the url to which the
                request for an authentication token will be submitted
            api_version - string representing the api version which will be
                used when making calls
        '''
        self.kiteworks_hostname = kiteworks_hostname
        self.client_app_id =client_app_id
        self.client_app_secret_key = client_app_secret_key
        self.signature_key = signature_key
        self.user_id = user_id
        self.upload_folder = upload_folder
        self.api_scope = api_scope
        self.redirect_uri = redirect_uri
        self.access_token_endpoint = access_token_endpoint
        self.access_token = self.get_access_token()
        self.api_version = '22'
        self.request_headers = {
            'Accept' : 'application/json',
            'X-Accellion-Version' : self.api_version,
            'Authorization' : 'Bearer ' + self.access_token,
        }
    def get_auth_code(self,timestamp:int,nonce:int):
        '''
        hashes and formats access credentials into an authorization code for submittal to
        the oauth2 server
        input parameters:
            timestamp - integer representing seconds since unix epoch
            nonce - random integer between 1 and 999999, inclusive
        '''
        base_string = '|@@|'.join([self.client_app_id,self.user_id,str(timestamp),str(nonce)])
        signature = hmac.new(self.signature_key.encode(),base_string.encode(),sha1).hexdigest()
        auth_code_list = [
            benc(bytes(self.client_app_id,'utf-8')).decode(),
            benc(self.user_id.encode()).decode(),
            str(timestamp),
            str(nonce),
            signature
        ]
        auth_code = '|@@|'.join(auth_code_list)
        return auth_code
    def get_access_token(self):
        '''
        hashes and submits credentials to the oauth2 server and retrieves a
        temporary token to use with api calls
        '''
        timestamp = int(dt.now().timestamp())
        nonce = randrange(1,1000000)
        auth_code = self.get_auth_code(timestamp,nonce)
        post_data = {
            'client_id' : self.client_app_id,
            'client_secret' : self.client_app_secret_key,
            'grant_type' : 'authorization_code',
            'code' : auth_code,
            'scope': self.api_scope,
            'redirect_uri' : self.redirect_uri,
        }
        response = requests.post(self.access_token_endpoint,post_data)
        if response.status_code==200:
            access_token = response.json()['access_token']
        else:
            access_token = ''
        return access_token
    def count_email(self):
        '''
        retrieves the count of email messages in each bucket (i.e., inbox,
        draft, outgoingQueued, outgoing, outgoingTransferring, inboxUnread,
        outgoingError, trash) for the user identified during authentication
        '''
        url = self.kiteworks_hostname + '/rest/mail/actions/counters'
        response = requests.get(url,headers=self.request_headers)
        return response
    def list_emails_from_date(self,receipt_date:dt=dt.now()):
        '''
        retrieves information about the inbox of the kiteworks user identified
        during authentication, and returns a list of emails received within the
        specified date range (inclusive dates)

        parameters:
            receipt_date - initial date of range to retrieve emails (inclusive)
        '''
        url = self.kiteworks_hostname + '/rest/mail'
        parameters = {
            'date' : receipt_date.strftime('%m/%d/%Y'),
        }
        response = requests.get(url,params=parameters,headers=self.request_headers)
        return response
    def list_email_in_date_range(self,start_date:dt=dt.now(),end_date:dt=dt.now()):
        '''
        retrieves information about the inbox of the kiteworks user identified
        during authentication, and returns a list of emails received within the
        specified date range (inclusive dates)

        parameters:
            start_date - initial date of range to retrieve emails (inclusive)
            end_date - final date of range to retieve emails (inclusive)
        '''
        url = self.kiteworks_hostname + '/rest/mail'
        parameters = {
            'date:gte' : start_date.strftime('%m/%d/%Y'),
            'date:lte' : end_date.strftime('%m/%d/%Y'),
        }
        response = requests.get(url,params=parameters,headers=self.request_headers)
        return response
    def get_message(self,mail_id:str):
        '''
        retrieves a single email message corresponding to the input id
        parameters and returns the response object.

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
        '''
        url = self.kiteworks_hostname + '/rest/mail/' + mail_id
        parameters = ''
        response = requests.get(url,params=parameters,headers=self.request_headers)
        # get key information from response data:
        email_data = response.json()
        {
            'sender' : email_data['subject'],
            'subject' : next(filter(lambda x: x['variable']=='SENDER_EMAIL',email_data['variables']))['value'],
            'body' : next(filter(lambda x: x['variable']=='BODY',email_data['variables']))['value'],
        }
        return response
    def list_attachments(self,email_id:str):
        '''
        retrieves a list of all attachments to the email message corresponding
        to the input email id

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
        '''
        url = self.kiteworks_hostname + '/rest/mail/{}/attachments'.format(email_id)
        response = requests.get(url,headers=self.request_headers)
        return response
    def preview_attachment(self,email_id:str,attachment_id:str):
        '''
        retrieves information about attachment from a specified email message
        corresponding to the input email and attachment ids

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
            attachment_id - unique identifier of an attachment to the email
                identified by mail_id
        '''
        url = self.kiteworks_hostname + '/rest/mail/{}/attachments/{}/preview'.format(email_id,attachment_id)
        response = requests.get(url,headers=self.request_headers)
        return response
    def get_attachment(self,email_id:str,attachment_id:str):
        '''
        retrieves a single attachment from a specified email message
        corresponding to the input email and attachment ids

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
            attachment_id - unique identifier of an attachment to the email
                identified by mail_id
        '''
        url = self.kiteworks_hostname + '/rest/mail/{}/attachments/{}/content'.format(email_id,attachment_id)
        response = requests.get(url,headers=self.request_headers)
        return response
    def download_attachment(self,email_id:str,attachment_id:str,download_path:Path):
        '''
        retrieves a single attachment from a specified email message
        corresponding to the input email and attachment ids and saves the
        attachment to the specified path

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
            attachment_id - unique identifier of an attachment to the email
                identified by mail_id
            download_path - path location to which the attachment will be saved
                upon successful download
        '''
        response = self.get_attachment(email_id,attachment_id)
        if response.status_code==200:
            with download_path.open('wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
        else:
            pass
    def download_attachments_as_zip(self,email_id:str,download_path:Path):
        '''
        retrieves all attachments from a specified email message
        corresponding to the input email ids as a single zip archive

        parameters:
            email_id - unique identifier of the message within the kiteworks
                system
            download_path - path object pointing to a location to save the
                attachment file once downloaded
        '''
        url = self.kiteworks_hostname + '/rest/mail/{}/attachments/actions/zip'.format(email_id)
        response = requests.get(url,self.request_headers)
        return response
    def upload_file(self,path:Path,folder:str):
        '''
        uploads a single file to the specified kiteworks folder.

        parameters:
            path - a path object pointing to a file which is to be uploaded to
                kiteworks ftp
            folder - a string representing the folder on kiteworks into which
                the file will be uploaded
        
        returns:
            file_id - a string representing the file as accessed on kiteworks.
        '''
        # helper function to break up large files into kilobyte chunks:
        chunk_size = 1024
        def chunkify(file_object):
            while True:
                chunk = file_object.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        # initiate a multi-chunk upload:
        url = self.kiteworks_hostname + '/rest/folders/' + self.upload_folder + '/actions/initiateUpload'
        file_size = path.stat().st_size
        number_of_chunks = file_size // chunk_size
        if file_size % chunk_size:
            number_of_chunks += 1
        post_data = {
            'filename' : path.name,
            'totalSize' : file_size,
            'totalChunks' : number_of_chunks
        }
        response = requests.post(url=url,headers=self.request_headers,data=post_data)
        upload_uri = response.json()['uri']
        file_id = response.json()['id']
        # loop through and upload file chunks:
        with path.open('rb') as f:
            url = self.kiteworks_hostname + '/' + upload_uri + '?returnEntity=true'
            for i,chunk in enumerate(chunkify(f)):
                post_data = {
                    'compressionMode' : 'NORMAL',
                    'compressionSize' : len(chunk),
                    'originalSize' : len(chunk),
                    'index' : i + 1,
                }
                files = {
                    'content' : chunk,
                }
                response = requests.post(url=url,files=files,headers=self.request_headers,data=post_data)
        return response.json()['id']

    def send_message(self,message:dict,paths:list):
        '''
        sends an email message with or without attachments via kiteworks' email
        service.
        
        parameters:
            message - a dictionary containing the following keys: 'to', 'cc',
                'bcc', 'subject', 'body', and 'files'. The items 'to', 'cc',
                and 'bcc' must have values defined as lists containing valid
                email addresses as strings. The 'subject' item must be defined
                as a string, and 'body' should be a string containing valid
                html.
            paths - a of path objects pointing to files to be uploaded to
                kiteworks and sent via email.
        '''
        if len(paths)>0:
            message['files'] = []
            # iterate through each file (ignores non-file items):
            for path in paths:
                if path.is_file():
                    file_id = self.upload_file(path,self.upload_folder)
                    message['files'].append(file_id)
        else:
            message['files'] = []
        url = self.kiteworks_hostname + '/rest/mail/actions/sendFile?returnEntity=true'
        response = requests.post(url=url,headers=self.request_headers,data=message)
        return response
    def whoami(self):
        '''
        retrieves api client information and returns the response object
        '''
        url = self.kiteworks_hostname + '/rest/clients/me'
        headers = {
            'accept' : 'application/json',
            'X-Accellion-Version' : self.api_version,
            'Authorization' : 'Bearer ' + self.access_token,
        }
        response = requests.get(url,headers=headers)
        return response