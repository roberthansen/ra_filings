Resource Adequacy Monthly/Annual Filing Validation Tool

2021-12-09
California Public Utilities Commission
Robert Hansen, PE


Introduction:
The Resource Adequacy Monthly/Annual Filing Validation Tool consists of four
Python scripts and two configuration files:
  Scripts:
    + ra_filings.py
    + kiteworks_scraper.py
    + ra_filing_organizer.py
    + ra_consolidator.py
    + logger.py
  Configuration Files:
    + ra_filings_config.yaml
    + lse_map.yaml
    + email_filter.yaml

The sections following discuss the usage of each of these components of the
tool. See comments within the scripts for additional information.

Finally, an additional file with login information is written in Python and
consists of a single dictionary containing a user id and password for accessing
Kiteworks. This information may be entered into the command line as an
alternative.


Quick Start Guide:
Update the filing_month value in ra_filings_config.yaml and run the following:
  > python ra_reports.py


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
          [mmm] : three-letter abbreviation for month of submittal
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
  browser - the name of the installed browser to use, e.g., firefox. Used when
      downloading monthly filings from the Kiteworks web interface.
  browser_action_timer - the time, specified as a decimal number of in seconds,
      between browser actions to account for loading times. Default is 0.75.
      Used when downloading monthly filings from the Kiteworks web interface.
  browser_action_retries - the number of times to attempt a browser action,
      such as clicking a button, before escaping. Used when downloading monthly
      filings from the Kiteworks web interface.
  log_file - the location of a file to which a log of actions will be saved.
      Used when any criticalities are identified for file logging and events
      of matching criticality occur.
  cli_logging_criticalities - a list of log criticality levels which will be
      reported to the command line interface. The available criticality levels,
      in order of descending severity, are ERROR, WARNING, and INFORMATION. The
      levels should be entered as a comma-separated list in all-caps and
      without spaces.
  file_logging_criticalities - a list of log criticality levels, as defined
      above, which will be recorded in the specified log file.

As a YAML file, the configuration settings can be edited with any text editor,
such as Notepad. Note that settings specifying a path such as a directory or
filename including filename_template generally should be enclosed in single
quotation marks. Other settings should not have quotation marks. See the YAML
specification for more information: https://yaml.org/spec/1.2.2/


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
This script copies data from various forecast and compliance filings into a
summary workbook which performs validation checks.


Login Information (login.py):
This Python file contains only a single dictionary with the following form,
used when automatically logging into Kiteworks:
  login_information = {
    'uid' : '[3-letter CPUC user ID]',
    'passwd' : '[CPUC user password]',
  }
The file is stored as plain text, thus posing a security risk. Alternative
methods for automating the login process are being investigated.