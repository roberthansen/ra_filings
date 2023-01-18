Resource Adequacy Monthly/Annual Filing Validation Tool

2021-12-09
California Public Utilities Commission
Robert Hansen, PE


Introduction:
The Resource Adequacy Monthly Filing Validation Tool consists of eleven Python
scripts, six Windows batch (.bat) scripts, and three configuration files:
  ./scripts/
    + ra_filings.py
    + california_state_holidays.py
    + configuration_options.py
    + data_extraction.py
    + export_to_ezdb.py
    + kiteworks_api.py
    + kiteworks_api_downloader.py
    + login.py
    + ra_consolidator.py
    + ra_logging.py
    + ra_organizer.py
  ./
    + ConsolidateRAFilings.bat
    + DailyRAFilings.bat
    + DownloadRAFilings.bat
    + ExportRAFilings.bat
    + NotifyRAFilings.bat
    + RunRAFilings.bat
  ./config/
    + ra_filings_config.yaml
    + organizations.yaml
    + email_filter.yaml

The sections following discuss the usage of each of these components of the
tool. See comments within the scripts for additional information.

The tool is encapsulated in a portable environment called an
Anaconda Project. The Anaconda Project definition consists of an additional
configuration file, anaconda-project.yml, which specifies the Python libraries
and versions required to run this tool. In addition, the project manages encrypted
environmental variables, including Kiteworks login information, which allows
the scripts to run automatically, securely, and without human intervention once
the variables are set.


Quick Start Guide:
Check and update values in ra_filings_config.yaml and run the following:
  > anaconda-project run daily


Windows Accounts:
This application is designed to be executed automatically on a daily schedule
using Windows' Task Scheduler. A service account, ENERGY\svc_energyRA, was
created to be the account through which the script would be invoked--this
account is independent of staff and is setup such that its password will not
expire, so the script can run indefinitely across, e.g., personnel changes.
ENERGY\svc_energyRA is not, accessible through Remote Desktop, but users may
create interactive sessions in PowerShell through the following command:
    runas.exe /profile /user:ENERGY\svc_energyRA "powershell -ExecutionPolicy ByPass"
A prompt will request the svc_energyRA password, and upon correct entry, a
new shell session will start as the ENERGY\svc_energyRA account. From there, the
user may execute any of the .ps1 scripts in the ra_filings directory using their
UNC paths:
    \\Sf150pyclfs26\PYCLIENTFS\Users\svc_energyRA\ra_filings\DailyRAFilings.ps1
These scripts map the network drive for the svc_energyRA user, activate a conda
environment, and execute anaconda commands with access to the service account's
keyring. This procedure is necessary for setting up the anaconda environment and
inputting Kiteworks API information.


Configuration File (ra_filings_config.yaml):
Using this tool involves preparing the configuration files as desired and
running the ra_reports.py script. The ra_filings_config.yaml file is a text
file written in the YAML Ain't Markup Language (YAML), and contains several
parameters that define the python scripts' behavior:
  filing_month -- the date of the filings to be evaluated, expressed as a month
      and year in the format mmm yyyy (e.g., "dec 2021"). The given date is
      used to open corresponding annual and monthly reports and filings.
      Quotation marks are not needed when inputting the date into the
      configuration file.
  planning_reserve_margin -- The margin of additional required capacity beyond
      forecasted load, for example 0.15 meaning LSEs will be required to
      provide 15% more capacity than the forecast load for a given month.
  demand_response_multiplier -- The coefficient to apply to capacity provided
      through demand response programs when assessing supply against forecast
      load
  transmission_loss_adder_pge --  The coefficient to apply to 
  lse_map_file -- The location of the lse map file, such as
          "'C:\Users\Myself\ResourceAdequacy\lse_map.csv'"
      Single quotation marks around paths are recommended, especially if the
      path contains spaces.
  filename_template -- A template for renaming reports based on their contents.
      The template should contain some form of the report date and the
      reporting Load Serving Entity. The following keywords are replaced with
      values from the report's Confirmation sheet, and other text is unchanged:
          [yyyy] : four digit year of submittal
          [yy] : last two digits of the year of submittal
          [mmmm] : full name of month of submittal
          ummm] : three-letter abbreviation for month of submittal
          [mm] : two-digit numeric month of submittal
          [lse_full] : full name of the submitting load serving entity as
              written in the report
          [lse_abbrev] : abbreviated name of the submitting Load Serving Entity
               from the lse map file
      The filename may include parent directories to help organize reports. The
      default filename tempalte is:
          "'[lse_full]_[yyyy]\MonthlyRAReport_[yyyy]-[mm]_[lse_abbrev].xlsx'"
      Single quotation marks around path templates are recommended, especially
      if the template includes spaces.
  temp_directory -- the directory in which attachments to emails in Kiteworks
      will be downloaded.
  ra_monthly_filing_filename_template -- a filename template, as described
      above, pointing to the current monthly filing for a given load serving
      entity. The file is read both when organizing downloaded files and
      when consolidating filings for summarization and validation.
  incremental_local_filename_template -- a filename template, as described
      above, pointing to the current annual incremental local resource
      forecast adjustments. The file is read when consolidating reports for
      validation of the load serving entities' monthly filings.
  cam_rmr_filename_template -- a filename template, as described above,
      pointing to the current monthly CAM-RMR report. The file is read when
      consolidating reports for validation of the load serving entities'
      monthly filings.
  ra_summary_filename_template -- a filename template, as described above,
      pointing to the current monthly resource adequacy summary report. This
      file includes the validation checks and is updated when consolidating
      reports and filings.
  month_ahead_filename_template -- a filename template, as described above,
      pointing to the current month-ahead load forecasts. The file is read
      when consolidating reports for validation of the load serving entities'
      monthly filings.
  year_ahead_filename_template -- a filename template, as described above,
      pointing to the current annual load forecasts. The file is read when
      consolidating reports for validation of the load serving entities'
      monthly filings.
  webdriver_directory -- the directory containing the webdriver executable
      file.
  browser -- the name of the installed browser to use, e.g., firefox. Used when
      downloading monthly filings from the Kiteworks web interface.
  browser_action_timer -- the time, specified as a decimal number of in
      seconds, between browser actions to account for loading times. Default is
      0.75. Used when downloading monthly filings from the Kiteworks web
      interface.
  browser_action_retries -- the number of times to attempt a browser action,
      such as clicking a button, before escaping. Used when downloading monthly
      filings from the Kiteworks web interface.
  log_file -- the location of a file to which a log of actions will be saved.
      Used when any criticalities are identified for file logging and events
      of matching criticality occur.
  cli_logging_criticalities -- a list of log criticality levels which will be
      reported to the command line interface. The available criticality levels,
      in order of descending severity, are ERROR, WARNING, and INFORMATION. The
      levels should be entered as a comma-separated list in all-caps and
      without spaces.
  file_logging_criticalities -- a list of log criticality levels, as defined
      above, which will be recorded in the specified log file.
  email_log_filename -- the location of a .csv file to which a log of Kiteworks
      emails will be saved. The log contains data about each email, such as
      receipt date, subject, sender, Kiteworks id, and whether the attachments
      are to be downloaded. Files placed manually in the download directories
      are also logged. This data can be used for tracing downloaded attachments
      to their sources. This log is used across multiple filing months.
  attachment_log -- the location of a .csv file to which attachments downloaded
      from emails to Kiteworks are logged. The log contains data about each
      attachment, such as download date, original filename, and associated
      email id. Files placed manually in the download directories are assigned
      unique ids for tracking purposes. Files recognized as relevant to the
      compliance check process are marked with the file type and copied with
      standardized filenames into relevant directories. This log is used across
      multiple filing months.
  consolidation_log_filename -- the location of a .csv file to which a log of
      each file used in assessing compliance for a single month is saved. The
      log consists of a list of each file expected during a compliance check
      with the file's status and, if the file exists, information for tracing
      to the source attachment and email. The log also includes the compliance
      status for each monthly filing. A different log is generated for each
      filing month when the ra_consolidator script is run, and the 
  version_controlled_files -- a list of file types to which version numbers are
      expected to be appended. The file types are referred to as 'ra_category'
      in ra_organizer.py and ra_consolidator.py, and correspond to the
      keys of the path_strings dictionary defined in the Paths class in
      configuration_options.py
  files_for_archive -- a list of files which will be copied into a zip archive
      when the ra_consolidator script is run.

The configuration settings can be edited with any text editor, such as Notepad.
Note that settings specifying a path such as a directory or filename including
filename_template generally should be enclosed in single quotation marks. Other
settings should not have quotation marks. See the YAML specification for more
information: https://yaml.org/spec/1.2.2/


Load Serving Entity Map (lse_map.yaml):
Load Serving Entities (LSEs) are responsible for submitting monthly filings,
and their name is included in the sheet labelled 'Certification' in their
filing workbooks. The lse_map.yaml contains a list of brief abbreviations of
each LSE's name, each followed by a sub-list of full-names, known alternate
spellings, and aliases. The abbreviations are used in renaming the report files
if specified in the filename_template, and must match the abbreviations in the
summary report. The map should be appended whenever a new LSE submits a report,
or when a known LSE submits a report with a new alias or spelling of their name.

The LSE map file can be edited using a text editor such as Notepad similar to
the configuration file. Any entries containing special characters (e.g.,
:{}[],&*#?|-<>=!%@\ ) should be enclosed in quotation marks. See the YAML
specification for more information : https://yaml.org/spec/1.2.2/


Email Filter Keywords (email_filter.yaml):
The webscraper can selectively download attachments only from emails according
to a set of keywords specified in the email filter keywords file. This file
contains two lists, one with keywords to include and one with keywords to
exclude. The filter applies these keywords such that the webscraper will
download attachments from emails containing any of the "include" keywords and
not matching any of the "exclude" keywords. All keywords are case-insensitive
but must otherwise match exactly, including spaces.


Resource Adequacy Filings Script (ra_filings.py):
This relatively simple script loads login information from a specified file and
initializes the other two scripts with the location of the configuration file.
The following command runs the script:
  > python ra_reports.py


Kiteworks Scraper (kiteworks_scraper.py):
This script defines a class which reads the configuration file into its own
variables and applies them when accessing the Kiteworks FTP email site through
the specified browser. The class uses the Python Selenium library to interface
with the browser's webdriver.

The Kiteworks scraper logs into Kiteworks using given authentication
information, then cycles through all unread emails checking subject lines
against optional filter keywords specified and downloading all attachments from
emails that pass the filter. Once all unread emails have been opened, the
scraper exits.

The configuration file allows a user to fine-tune the scraper according to
their needs and performance. For instance, if certain Kiteworks pages take a
long time to load, causing the scraper to checking emails, the
browser_action_timer and browser_action_retries paramaters can be increased to
allow a longer time between attempting actions such as clicking a button, or to
allow more attempts at a given action before either returning to the inbox or
exiting the scraper.


Resource Adequacy Filing Organizer (ra_filing_organizer.py):
This script reads through the entire contents of the temp_directory, first
decompressing any zip archives, then searching for files matching the Resource
Adequacy Monthly/Annual Report template. Any matching files are copied to the
report_directory and renamed according to the report's contents and the
filename_template.

Resource Adequacy Consolidator (ra_consolidator.py)
This script performs data validation and copies data from various forecast and
compliance filings into two summary workbooks.


Login Information (login.py):
This Python file retrieves login information from environment variables set
when 'anaconda-project run' is executed. The login information object is loaded
into a dict for use in the Kiteworks webscraper:
  login_information = {
    'uid' : '[3-letter CPUC user ID or email address]',
    'passwd' : '[CPUC user password]',
  }
Persistent storage of the login credentials are handled through Anaconda
Project's  environment variable tools, which provides access to the host
operating system's secure, encrypted keyring. The following commands, executed
in PowerShell from the project directory with a conda environment activate
un-sets the user id and password, respectively:
  > anaconda-project set-variable KITEWORKS_UID_SECRET=[user id]
  > anaconda-project set-variable KITEWORKS_PASSWD_SECRET=[password]
After un-setting the login credential variables, they must be re-input by
executing the following command and inputting the new values in the prompt:
  > anaconda-project prepare


Troubleshooting:
Here are a few issues that have come up during usage and their somewhat
unintuitive solutions.

SSL Certification - Python uses the "certifi" library to handle ssl/tsl
certification. The library does not automatically retrieve the certificate from
kwftp.cpuc.ca.gov, so it is necessary to copy the certificate from a web browser
when using a fresh conda environment, when the current certificate expires, or
when the website obatins a new certificate. The certificate for
kwftp.cpuc.ca.gov must be copied into the file located at
"./envs/default/Lib/site-packages/certifi/cacert.pem"

Excel File GUIDs - In some cases, Load Serving Entities have submitted
their monthly resource adequacy filings in Excel files with a Globally Unique
Identifier (GUID, aka UUID) code containing lower-case letters. While the GUID
specification generally permits hexidecimal values including either upper- or
lower-case letters, the version of openpyxl used during development includes
a regular expression (regex) test that includes only upper-case letters, thus, while
Microsoft Excel has no trouble opening the file, the Python scripts are unable
to read the file. This issue is resolved by finding the regex match string in
the following openpyxl library file within the conda environment:
"./envs/default/Lib/site-packages/openpyxl/descriptors/excel.py"
Line 91 in this file is a regex pattern to be used in defining the "Guid" class,
and should be changed to include "a-f" in each set of square brackets as
follows:
    pattern = r"{[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}\}"
    pattern = r"{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}"

