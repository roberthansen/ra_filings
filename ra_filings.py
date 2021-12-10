from pathlib import Path
from ra_filing_organizer import ra_filing_organizer
from ra_consolidator import ra_consolidator
from login import user

# 2021-12-09
# California Public Utilities Commission
# Robert Hansen, PE

# download and organize resource adequacy monthly/annual reports
def ra_filings(configuration_path:Path,skip_download:bool=False):
    # download all attachments from unread emails in kiteworks:
    if not skip_download:
        import kiteworks_scraper
        kw = kiteworks_scraper(configuration_path=configuration_path,user=user)
        kw.retrieve_emails()
    else:
        pass
    
    # organize downloaded attachments into final report directory:
    org = ra_filing_organizer(configuration_path=configuration_path)
    org.organize()

    # consolidate data from filings and requirement tables:
    rv = ra_consolidator(configuration_path=configuration_path)
    rv.clear_data_ranges()
    rv.consolidate_allocations()
    rv.consolidate_filings()


if __name__=='__main__':
    ra_filings(Path(r'C:\Users\rh2\Documents\src\python\resource_adequacy_reports\ra_filings_config.yaml'),skip_download=True)