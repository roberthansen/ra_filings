import getpass
import keyboard
import datetime as dt
from functools import reduce
from yaml import safe_load
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from logger import logger

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

# class to access cpuc's kiteworks secure email client through a browser and download attachments from all unread emails to a specified directory. intended to be used for organizing resource adequacy monthly/annual reports.
class kiteworks_scraper:
    site_url = 'https://kwftp.cpuc.ca.gov'

    # initialize object:
    def __init__(self,configuration_path: Path=Path.cwd()/'config.txt',login_information: dict=dict()):
        self.configuration_options = {
            'email_filter_file' : Path.cwd(),
            'temp_directory' : Path.cwd(),
            'webdriver_directory' : Path.cwd(),
            'browser' : 'firefox',
            'browser_action_timer' : 1.0,
            'browser_action_retries' : 5,
            'log_file' : Path.cwd() / 'download_organizer.log',
            'cli_logging_criticalities' : [],
            'file_logging_criticalities' : [],
        }
        self.logger = logger()
        self.set_configuration_options(configuration_path)
        self.set_login_information(login_information)

    # set email filter keywords:
    def set_email_filter(self,p: Path=Path.cwd()):
        self.email_filter = {
            'include' : [],
            'exclude' : [],
        }
        if p.is_file():
            self.email_filter_path = p
            self.email_filter = dict()
            with self.email_filter_path.open(mode='r') as f:
                d = safe_load(f)
                for key in d.keys():
                    value = d[key]
                    if key.lower() in ('include', 'exclude'):
                        self.email_filter[key.lower()] = [s.lower() for s in value]
                    else:
                        pass
            self.logger.log('Applying Email Filter Options from {}'.format(p),'INFORMATION')
        else:
            self.email_filter_path = None
            self.logger.log('No Email Filter Options Found, Will Download Attachments from All Unread Emails','WARNING')
            

    # set directory for storing email attachments:
    def set_download_directory(self,p: Path=Path.cwd()):
        if p.is_dir():
            self.download_directory = p
            self.logger.log('Downloading Attachments to {}'.format(p),'INFORMATION')
        else:
            self.download_directory = Path.cwd()
            self.logger.log('Specified Download Directory Not Found, Using {}'.format(Path.cwd()),'WARNING')

    # set object property with dict containing a user id and password for kiteworks:
    def set_login_information(self,login_information:dict=dict()):
        self.login_information = dict()
        if login_information:
            self.login_information = login_information
        else:
            self.login_information['uid'] = input('3-Digit CPUC ID: ')
            self.login_information['passwd'] = getpass.getpass(prompt='CPUC Password: ')

    # set browser to use:
    def set_browser(self,browser='firefox'):
        if browser.lower() in ['chrome','edge','firefox','ie']:
            self.browser = browser.lower()
        else:
            self.browser = 'firefox'
            self.logger.log('Specified Browser Not Available, Using Firefox','WARNING')

    # set directory containing webdriver executable:
    def set_webdriver_directory(self,p: Path=Path.cwd()):
        if p.is_dir():
            self.webdriver_directory = p
        else:
            self.webdriver_directory = Path.cwd()
            self.logger.log('Specified Webdriver Directory Not Found, Using {}'.format(self.webdriver_directory),'WARNING')

    # set the minimum time between browser actions:
    def set_action_timer(self,duration=1.0):
        self.action_timer = duration

    # set the maximum number of retries for any step in the process:
    def set_maximum_retries(self,n=5):
        self.maximum_retries = n

    # read configuration file and apply relevant options:
    def set_configuration_options(self,p: Path):
        if p.is_file():
            self.configuration_path = p
            with self.configuration_path.open(mode='r') as f:
                d = safe_load(f)
                for key in d.keys():
                    if key in self.configuration_options.keys():
                        value = d[key]
                        if 'criticalities' in key:
                            self.configuration_options[key] = value.split(',')
                        elif 'file' in key or 'directory' in key:
                            self.configuration_options[key] = Path(value)
                        else:
                            self.configuration_options[key] = value
                    else:
                        pass
            self.logger.log('Applying Configuration Options from {}'.format(p),'INFORMATION')
        else:
            self.configuration_path = None
            self.logger.log('Applying Default Configuration---Unable to Load Options from {}'.format(p),'WARNING')
        self.logger.set_cli_logging_criticalities(self.configuration_options['cli_logging_criticalities'])
        self.logger.set_file_logging_criticalities(self.configuration_options['file_logging_criticalities'])
        self.set_email_filter(self.configuration_options['email_filter_file'])
        self.set_download_directory(Path(self.configuration_options['temp_directory']))
        self.set_browser(self.configuration_options['browser'])
        self.set_webdriver_directory(Path(self.configuration_options['webdriver_directory']))
        self.set_action_timer(self.configuration_options['browser_action_timer'])
        self.set_maximum_retries(self.configuration_options['browser_action_retries'])

    # create a webdriver object based on the browser type:
    def webdriver(self):
        mime_types = [
            'text/plain',
            'text/csv',
            'application/zip',
            'application/octet-stream',
            'application/x-zip',
            'application/x-zip-compressed',
            'application/excel',
            'application/word',
            'application/pdf',
        ]
        if self.browser=='chrome':
            driver_location = self.webdriver_directory / 'chromedriver.exe'
            ser = webdriver.chrome.service.Service(driver_location)
            opts = webdriver.ChromeOptions()
        elif self.browser=='edge':
            driver_location = self.webdriver_directory / 'msedgedriver.exe'
            ser = webdriver.edge.service.Service(driver_location)
            opts = webdriver.EdgeOptions()
        elif self.browser=='firefox':
            driver_location = self.webdriver_directory / 'geckodriver.exe'
            ser = webdriver.firefox.service.Service(driver_location)
            opts = webdriver.FirefoxOptions()
        opts.set_preference('browser.download.panel.shown',False)
        opts.set_preference('browser.helperApps.neverAsk.saveToDisk',';'.join(mime_types))
        opts.set_preference('browser.helperApps.alwaysAsk.force',False)
        opts.set_preference('browser.download.manager.showWhenStarting',False)
        opts.set_preference('browser.download.folderList',2)
        opts.set_preference('browser.download.dir',str(self.download_directory))
        if self.browser=='chrome':
            driver = webdriver.Chrome(service=ser,options=opts)
        elif self.browser=='edge':
            driver = webdriver.Edge(service=ser,options=opts)
        elif self.browser=='firefox':
            driver = webdriver.Firefox(service=ser,options=opts)
        return driver

    # log into kiteworks and retrieve attachments to all unread emails:
    def retrieve_emails(self):
        if self.login_information['uid']=='':
            self.set_login_information()
        try:
            with self.webdriver() as driver:
                driver.get(self.site_url)
                t0 = dt.datetime.now()
                t = lambda: (dt.datetime.now() - t0).total_seconds()
                tf = True
                state = 0
                retry_counter = 0
                initial_download_count = len(list(self.download_directory.iterdir()))
                download_count = initial_download_count
                # non-blocking loop through states to open browser, log into
                # kiteworks, check unread emails, and download attachments:
                while tf:
                    # check whether document has completely loaded before attempting to access elements:
                    document_ready = driver.execute_script('return document.readyState;')=='complete'
                    # get url for current page:
                    try:
                        url = driver.current_url
                    except:
                        url = ''
                    # escape or print status upon user request:
                    try:
                        if keyboard.is_pressed('q'):
                            self.logger.log('exiting ...','INFORMATION')
                            state = 99
                            tf = False
                        if keyboard.is_pressed('s'):
                            self.logger.log('URL: {}\nSTATE: {}\nPAGE LOADED: {}\nTIMER: {}\nRETRIES: {}'.format(url,state,document_ready,t(),retry_counter))
                        else:
                            pass
                    except:
                        self.logger.log('press \'q\' to exit','INFORMATION')
                    # state 0 - enter user id:
                    if state==0 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Entering User ID ...','INFORMATION')
                            driver.find_element(By.ID,'email').send_keys(self.login_information['uid'])
                            retry_counter = 0
                            state = 1
                        except:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('Unable to Enter User ID','ERROR')
                                state = 99
                            else:
                                pass
                    # state 1 - login screen --- submit user id:
                    elif state==1 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Submitting User ID ...','INFORMATION')
                            driver.find_element(By.ID,'email').send_keys(Keys.RETURN)
                            retry_counter = 0
                            state = 2
                        except:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('Unable to Submit User ID','ERROR')
                                state = 99
                            else:
                                pass
                    # state 2 - login screen --- enter user password:
                    elif state==2 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Entering Password ...','INFORMATION')
                            driver.find_element(By.ID,'password').send_keys(self.login_information['passwd'])
                            retry_counter = 0
                            state = 3
                        except:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('Unable to Entering Password','ERROR')
                                state = 99
                            else:
                                pass
                    # state 3 - login screen --- submit user password:
                    elif state==3 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Submitting Password ...','INFORMATION')
                            driver.find_element(By.ID,'password').send_keys(Keys.RETURN)
                            retry_counter = 0
                            state = 4
                        except:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('Unable to Submit Password','ERROR')
                                state = 99
                            else:
                                pass
                    # state 4 - email inbox --- open filter menu:
                    elif state==4 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        # check that url matches state:
                        if url.split('/')[-1] == 'inbox':
                            try:
                                self.logger.log('Accessing Email Filter Menu ...','INFORMATION')
                                driver.find_element(By.XPATH,'//div[@class=\'kw-mail-filter\']/div[1]/button[@aria-label=\'Filter\']').click()
                                retry_counter = 0
                                state = 5
                            except:
                                if retry_counter >= self.maximum_retries:
                                    self.logger.log('Unable to Access Email Filter Menu','ERROR')
                                    state = 99
                                else:
                                    pass
                        else:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('State Mismatch','WARNING')
                                retry_counter = 0
                                state = 9
                            else:
                                pass
                    # state 5 - email inbox  --- select unread filter:
                    elif state==5 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        # check that url matches state:
                        if url.split('/')[-1] == 'inbox':
                            try:
                                self.logger.log('Selecting \'Unread\' Email Filter ...','INFORMATION')
                                driver.find_element(By.XPATH,'//ul[@class=\'kw-filter-option-dropdown unstyled\']/li[@aria-label=\'Unread\']').click()
                                retry_counter = 0
                                state = 6
                            except:
                                self.logger.log('Unable to Select \'Unread\' Email Filter','INFORMATION')
                                retry_counter = 0
                                state = 4
                        else:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('State Mismatch','WARNING')
                                retry_counter = 0
                                state = 9
                            else:
                                pass
                    # state 6 - email inbox --- open first email:
                    elif state==6 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        # check that url matches state:
                        if url.split('/')[-1] == 'inbox':
                            # check if inbox is empty:
                            try:
                                self.logger.log('Checking for Empty Inbox ...','INFORMATION')
                                driver.find_element(By.CLASS_NAME,'ml-no-item-message-title')
                                emails = False
                            except:
                                emails = True
                            if emails:
                                try:
                                    self.logger.log('Opening Top Email ...','INFORMATION')
                                    driver.find_element(By.CLASS_NAME,'ml-listing__subject-col').click()
                                    retry_counter = 0
                                    state = 7
                                except:
                                    if retry_counter >= self.maximum_retries:
                                        self.logger.log('Unable to Open Email','ERROR')
                                        state = 99
                                    else:
                                        pass
                            else:
                                self.logger.log('No Unread Emails, Exiting ...','INFORMATION')
                                state =  98
                        else:
                            if retry_counter >= self.maximum_retries:
                                self.logger.log('State Mismatch','WARNING')
                                retry_counter = 0
                                state = 9
                            else:
                                pass
                    # state 7 - check whether attachments are expired:
                    elif state==7 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        try:
                            expiration_notification = driver.find_element(By.CLASS_NAME,'mail-info-expiration--expired').text
                            self.logger.log(expiration_notification,'INFORMATION')
                            retry_counter = 0
                            state = 9
                        except:
                            self.logger.log('No Expiration Notification Detected','INFORMATION')
                            state = 8
                    # state 8 - download attachments:
                    elif state==8 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            email_subject = driver.find_element(By.XPATH,'//div[@class=\'mail-info-subject\']/span[@class=\'subject-text\']').text
                            email_sender = driver.find_element(By.CLASS_NAME,'mail-info-sender__email').text
                            email_text = driver.find_element(By.CLASS_NAME,'mail-info-body').text
                        except:
                            self.logger.log('Unable to Retrieve Email Contents','WARNING')
                            email_subject = '[no subject found]'
                            email_sender = '[unknown sender]'
                        # check email subject against include email filter list:
                        if isinstance(self.email_filter['include'],list):
                            include = reduce(lambda x,y:x|y,[s in email_subject.lower() for s in self.email_filter['include']],False)
                        else:
                            include = True
                        if isinstance(self.email_filter['exclude'],list):
                            exclude = reduce(lambda x,y:x|y,[s in email_subject.lower() for s in self.email_filter['exclude']],False)
                        else:
                            exclude = False
                        # check for no attachments:
                        try:
                            self.logger.log('Checking for No Attachments Label ...','INFORMATION')
                            has_attachments = not (driver.find_element(By.XPATH,'//div[@class=\'mail-info-users\']/span[1]').text=='No attachments')
                        except:
                            has_attachments = True
                        # download attachments if email subject passes filter:
                        if has_attachments and include and not exclude:
                            try:
                                self.logger.log('Downloading Attachments --- {} from {}'.format(email_subject,email_sender),'INFORMATION')
                                driver.find_element(By.XPATH,'//button[@aria-label=\'Download\']').click()
                                download_count += 1
                                retry_counter = 0
                                state = 9
                            except:
                                if retry_counter>=self.maximum_retries:
                                    # check if browser is in inbox and set state to match:
                                    if url.split('/')[-1] == 'inbox':
                                        self.logger.log('State Mismatch','WARNING')
                                        state = 4
                                    else:
                                        self.logger.log('Unable to Download Attachements','WARNING')
                                        state = 9
                                    retry_counter = 0
                                else:
                                    self.logger.log('Unable to Download Attachments, Rechecking for No Attachments Label','WARNING')
                                    state = 7
                        elif exclude or not include:
                            self.logger.log('Email Subject Does Not Pass Filter Arguments','INFORMATION')
                            retry_counter = 0
                            state = 9
                        else:
                            self.logger.log('No Attachments','INFORMATION')
                            retry_counter = 0
                            state = 9
                    # state  9 - return to inbox:
                    elif state==9 and t()>self.action_timer and document_ready:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Returning to Inbox ...','INFORMATION')
                            driver.find_element(By.XPATH,'//tr[@aria-label=\'Inbox\']/td[1]/div[@class=\'title\']').click()
                            state = 4
                        except:
                            if retry_counter>=self.maximum_retries:
                                self.logger.log('Unable to Return to Inbox')
                            else:
                                pass
                    # state 98 - wait for downloads to complete, then exit:
                    elif state==98 and t()>self.action_timer:
                        t0 = dt.datetime.now()
                        retry_counter += 1
                        if retry_counter>=self.maximum_retries:
                            self.logger.log('{} Downloads Have Not Completed'.format(download_count-initial_download_count),'WARNING')
                        if len(list(self.download_directory.iterdir()))>=download_count:
                            self.logger.log('Downloads complete','INFORMATION')
                            state = 99
                    # state 99 - exit while loop and close webdriver:
                    elif state==99:
                        tf = False
                    else:
                        pass
        except OSError:
            self.logger.log('Webdriver not found for {} browser at {}'.format(self.browser,self.webdrivers[self.browser]['path'](self)))
