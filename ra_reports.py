from pathlib import Path
from kiteworks_scraper import kiteworks_scraper
from ra_report_organizer import ra_report_organizer
from login import login_information as li

# 2021-11-04
# California Public Utilities Commission
# Robert Hansen, PE

# download and organize resource adequacy monthly/annual reports
def ra_reports(config_file: Path):
    # download all attachments from unread emails in kiteworks:
    kw = kiteworks_scraper(login_information = li)
    kw.set_configuration_options(config_file)
    kw.retrieve_emails()
    
    # organize downloaded attachments into final report directory:
    org = ra_report_organizer()
    org.set_configuration_options(config_file)
    org.organize()

if __name__=='__main__':
    ra_reports(Path('C:/Users/rober/Documents/src/python/kiteworks_scraper/ra_reports.config'))