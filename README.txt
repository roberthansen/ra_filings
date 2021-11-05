Resource Adequacy Monthly/Annual Report Collector

2021-11-05
California Public Utilities Commission
Robert Hansen, PE

Introduction:
The Resource Adequacy Monthly/Annual Report Collector tool consists of three
Python scripts and two configuration files:
  Scripts:
    + ra_reports.py
    + kiteworks_scraper.py
    + ra_report_organizer.py
  Configuration Files:
    + ra_reports.config
    + lse_map.csv
An additional file with login information is written in Python and consists
of a single dictionary containing a user id and password for accessing
Kiteworks. This information may be entered into the command line as an
alternative.

The following sections discuss the usage of each of these components of the
tool. See comments within the scripts for additional information.

Configuration File:
Using the tool involves setting up the configuration files as desired, and
running the ra_reports.py script. The ra_reports.config file is a plain text
file containing several parameters that define the python scripts' behavior:
  lse_map_file -- The location of the lse map file, such as
      C:\Users\Myself\ResourceAdequacy\lse_map.csv
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
          [lse_full]_[yyyy]\MonthlyRAReport_[yyyy]-[mm]_[lse_abbrev].xlsx
  temp_directory -- the directory in which attachments to emails in Kiteworks
      will be downloaded.
  report_directory -- the directory in which renamed reports will be saved.
  webdriver_directory -- the directory containing the webdriver executable
      file.
  browser - the name of the installed browser to use, e.g., firefox
  browser_action_timer - the time, specified as a decimal number of in seconds,
      between browser actions to account for loading times. Default is 0.75
  browser_action_retries - the number of times to attempt a browser action,
      such as clicking a button, before escaping.
  log_file - the location of a file to which a log of actions will be saved.
  cli_logging_criticalities - a list of log criticality levels which will be
      reported to the command line interface. The available criticality levels
      are ERROR, WARNING, and INFORMATION. The levels should be entered as a
      comma-separated list without spaces.
  file_logging_criticalities - a list of log criticality levels which will be
      recorded in the log file.

Load Serving Entity Map:
Load Serving Entities (LSEs) are responsible for submitting monthly reports,
and their name is included in the Certification sheet of their report files.
The lse_map.csv is a table of known spellings of LSE names and a brief
abbreviation of their name potentially used in renaming the report file if
specified in the filename_template. The table should be appended whenever a new
LSE submits a report or a known LSE submits a report with a novel spelling of
their name. As a .csv file, the table may be edited either in a spreadsheet
editor, such as Excel, or using a text editor.

Resource Adequacy Reports Script (ra_reports.py):
This relatively simple script loads login information from a specified file and
initializes the other two scripts with the location of the configuration file.
The following command runs the script:
  > python ra_reports.py

Kiteworks Scraper (kiteworks_scraper.py):
This script defines a class which reads the configuration file into its own
variables and applies them when accessing the Kiteworks FTP email site through
the specified browser. The class uses the Python Selenium library to interface
with the browser's webdriver.

Resource Adequacy Report Organizer (ra_report_organizer.py):
This script reads through the entire contents of the temp_directory, first
decompressing any zip archives, then searching for files matching the Resource
Adequacy Monthly/Annual Report template. Any matching files are copied to the
report_directory and renamed according to the report's contents and the
filename_template.

Login Information (login.py):
This Python file contains only a single dictionary with the following form,
used when automatically logging into Kiteworks:
  login_information = {
    'uid' : '[3-letter CPUC user ID]',
    'passwd' : '[CPUC user password]',
  }
The file is stored as plain text, thus posing a security risk. Alternative
methods for automating the login process are being investigated.