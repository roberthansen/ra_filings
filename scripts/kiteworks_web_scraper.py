import re
import getpass
from datetime import datetime as dt
from functools import reduce
from yaml import safe_load
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from configuration_options import ConfigurationOptions,Paths,EmailFilter
from ra_logging import TextLogger
from scripts.configuration_options import ConfigurationOptions

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

# class to access cpuc's kiteworks secure email client through a browser and download attachments from all unread emails to a specified directory. intended to be used for organizing resource adequacy monthly/annual reports.
class KiteworksWebScraper:
    site_url = 'https://kwftp.cpuc.ca.gov'

    # initialize object:
    def __init__(self,configuration_path:Path,user:dict):
        self.configuration_options = ConfigurationOptions(configuration_path)
        self.paths = Paths(self.configuration_options)
        self.email_filter = EmailFilter(self.paths.get_path('email_filter'))
        self.logger = TextLogger(
            self.configuration_options.get_option('cli_logging_criticalities'),
            self.configuration_options.get_option('file_logging_criticalities'),
            self.paths.get_path('log')
        )
        self.set_user(user)
        self.download_source = 'external'
        self.set_browser(self.configuration_options.get_option('browser'))
        self.set_action_timer(self.configuration_options.get_option('browser_action_timer'))
        self.set_maximum_retries(self.configuration_options.get_option('browser_action_retries'))

    # set directory for storing email attachments based on internal or external source:
    def set_download_source(self,driver,s:str='external'):
        if s in ('internal','external') and s!=self.download_source:
            path = self.paths.get_path('downloads_{}'.format(s))
            driver.execute_script('window.open(\'about:blank\');')
            driver.switch_to_window(driver.window_handles[1])
            driver.get('about:config')
            try:
                driver.find_element_by_id('warningButton').click()
            except:
                pass
            driver.execute_script(
                'Components.classes[\'@mozilla.org/preferences-service;1\']' + \
                '.getService(Components.interfaces.nsIPrefBranch)' + \
                '.setStringPref(\'browser.download.dir\',\'{}\');'.format(str(path).replace('\\','\\\\'))
                )
            driver.execute_script('window.close();')
            driver.switch_to_window(driver.window_handles[0])
            self.download_source = s
            self.logger.log('Setting Download Directory to {}'.format(path),'INFORMATION')
        else:
            pass

    # set object property with dict containing a user id and password for kiteworks:
    def set_user(self,user:dict=dict()):
        self.user = dict()
        if user:
            self.user = user
        else:
            self.user['uid'] = input('3-Digit CPUC ID: ')
            self.user['passwd'] = getpass.getpass(prompt='CPUC Password: ')

    # set browser to use:
    def set_browser(self,browser='firefox'):
        if browser.lower() in ['chrome','edge','firefox','ie']:
            self.browser = browser.lower()
        else:
            self.browser = 'firefox'
            self.logger.log('Specified Browser Not Available, Using Firefox','WARNING')

    # set the minimum time between browser actions:
    def set_action_timer(self,duration=1.0):
        self.action_timer = duration

    # set the maximum number of retries for any step in the process:
    def set_maximum_retries(self,n=5):
        self.maximum_retries = n

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
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'application/vnd.ms-excel.sheet.macroEnabled.12',
            'application/word',
            'application/pdf',
        ]
        if self.browser=='chrome':
            driver_location = self.paths.get_path('webdrivers') / 'chromedriver.exe'
            ser = webdriver.chrome.service.Service(driver_location)
            opts = webdriver.ChromeOptions()
            opts.add_experimental_option('prefs',{
                'download.default_directory' : str(self.paths.get_path('downloads')),
            })
            opts.add_extension()
            driver = webdriver.Chrome(service=ser,chrome_options=opts)
        elif self.browser=='edge':
            driver_location = self.paths.get_path('webdrivers') / 'msedgedriver.exe'
            ser = webdriver.edge.service.Service(driver_location)
            opts = webdriver.EdgeOptions()
            driver = webdriver.Edge(service=ser,options=opts)
        elif self.browser=='firefox':
            driver_location = str(self.paths.get_path('webdrivers') / 'geckodriver.exe')
            opts = webdriver.FirefoxOptions()
            opts.set_preference('browser.download.panel.shown',False)
            opts.set_preference('browser.helperApps.neverAsk.saveToDisk',';'.join(mime_types))
            opts.set_preference('browser.helperApps.alwaysAsk.force',False)
            opts.set_preference('browser.download.manager.showWhenStarting',False)
            opts.set_preference('browser.download.folderList',2)
            opts.set_preference('browser.download.dir',str(self.paths.get_path('downloads_external')))
            driver = webdriver.Firefox(executable_path=driver_location,options=opts)
        return driver

    # log into kiteworks and retrieve attachments to all unread emails:
    def retrieve_emails(self):
        internal_address_check = re.compile(r'\S*@cpuc\.ca\.gov')
        if self.user['uid']=='':
            self.set_user()
        try:
            with self.webdriver() as driver:
                driver.get(self.site_url)
                t0 = dt.now()
                t = lambda: (dt.now() - t0).total_seconds()
                tf = True
                state = 0
                retry_counter = 0
                initial_download_count = len(list(self.paths.get_path('downloads_internal').iterdir())) + len(list(self.paths.get_path('downloads_external').iterdir()))
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
                    # state 0 - enter user id:
                    if state==0 and t()>self.action_timer and document_ready:
                        t0 = dt.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Entering User ID ...','INFORMATION')
                            driver.find_element(By.ID,'email').send_keys(self.user['uid'])
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
                        t0 = dt.now()
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
                        t0 = dt.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Entering Password ...','INFORMATION')
                            driver.find_element(By.ID,'password').send_keys(self.user['passwd'])
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
                        t0 = dt.now()
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
                        t0 = dt.now()
                        retry_counter += 1
                        # check that url matches state:
                        if url.split('/')[-1] == 'inbox':
                            try:
                                self.logger.log('Accessing Email Filter Menu ...','INFORMATION')
                                driver.find_element(By.XPATH,'//div[@class=\'kw-mail-filter\']/div[1]/button[@aria-label=\'Filter\']').click()
                                state = 5
                            except:
                                if retry_counter >= self.maximum_retries:
                                    self.logger.log('Unable to Access Email Filter Menu','ERROR')
                                    state = 9
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
                        t0 = dt.now()
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
                                driver.find_element(By.TAG_NAME,'body').send_keys(Keys.F5)
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
                        t0 = dt.now()
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
                                    # driver.find_element(By.CLASS_NAME,'ml-listing__subject-col').click()
                                    driver.find_element(By.CLASS_NAME,'ml-listing__row--unread').click()
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
                        t0 = dt.now()
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
                        t0 = dt.now()
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
                        include_email = self.email_filter.check_email_subject(email_subject)
                        # check for no attachments:
                        try:
                            self.logger.log('Checking for No Attachments Label ...','INFORMATION')
                            has_attachments = not (driver.find_element(By.XPATH,'//div[@class=\'mail-info-users\']/span[1]').text=='No attachments')
                        except:
                            has_attachments = True
                        # download attachments if email subject passes filter:
                        if has_attachments and include_email:
                            try:
                                if internal_address_check.match(email_sender):
                                    self.set_download_source(driver,'internal')
                                else:
                                    self.set_download_source(driver,'external')
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

                        elif not include_email:
                            self.logger.log('Email Subject Does Not Pass Filter Arguments: {}'.format(email_subject),'INFORMATION')
                            retry_counter = 0
                            state = 9
                        else:
                            self.logger.log('No Attachments','INFORMATION')
                            retry_counter = 0
                            state = 9
                    # state  9 - return to inbox:
                    elif state==9 and t()>self.action_timer and document_ready:
                        t0 = dt.now()
                        retry_counter += 1
                        try:
                            self.logger.log('Returning to Inbox ...','INFORMATION')
                            driver.get(self.site_url + '/#/mail/inbox')
                            driver.refresh()
                            state = 4
                            retry_counter = 0
                        except:
                            if retry_counter>=self.maximum_retries:
                                self.logger.log('Unable to Return to Inbox')
                            else:
                                pass
                    # state 98 - wait for downloads to complete, then exit:
                    elif state==98 and t()>self.action_timer:
                        t0 = dt.now()
                        retry_counter += 1
                        if retry_counter>=self.maximum_retries:
                            self.logger.log('{} Downloads Have Not Completed'.format(download_count-initial_download_count),'WARNING')
                        if len(list(self.paths.get_path('downloads_internal').iterdir()))+len(list(self.paths.get_path('downloads_external').iterdir()))>=download_count:
                            self.logger.log('Downloads complete','INFORMATION')
                            state = 99
                    # state 99 - exit while loop and close webdriver:
                    elif state==99:
                        tf = False
                    else:
                        pass
        except OSError:
            self.logger.log('Webdriver not found for {} browser at {}'.format(self.browser,self.paths.get_path('webdrivers')))