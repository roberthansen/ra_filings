from pathlib import Path

from ra_consolidator import WorkbookConsolidator
from login import kw_user,kw_api_client
from ra_organizer import Organizer

# 2021-12-09
# California Public Utilities Commission
# Robert Hansen, PE

# download and organize resource adequacy monthly/annual reports
def ra_filings(configuration_path:Path,use_api:bool=True,skip_download:bool=False,skip_organize:bool=False,skip_consolidate:bool=False,skip_notify:bool=False):
    '''
    this function is the primary means of interacting with the resource
    adequacy monthly filing compliance tool.
    '''
    cleared_log = False

    # download all attachments from unread emails in kiteworks:
    if not skip_download or not skip_notify:
        if use_api:
            from kiteworks_api_downloader import AttachmentDownloader
            kw = AttachmentDownloader(configuration_path=configuration_path,user=kw_user,api_client=kw_api_client)
            kw.logger.clear_log()
            if not skip_download:
                kw.download_filing_month()
            else:
                pass
        else:
            from kiteworks_web_scraper import KiteworksWebScraper
            kw = KiteworksWebScraper(configuration_path=configuration_path,user=kw_user)
            kw.retrieve_emails()
    else:
        pass
    
    # organize downloaded attachments into final report directory:
    org = Organizer(configuration_path=configuration_path)
    if not skip_organize:
        if not cleared_log:
            org.logger.clear_log
        org.organize()
    else:
        pass

    # consolidate data from filings and requirement tables:
    if not skip_consolidate:
        cons = WorkbookConsolidator(configuration_path=configuration_path)
        if not cleared_log:
            cons.logger.clear_log()
        cons.consolidation_logger.clear_log()
        ready = cons.check_files()
        if ready:
            missing_filings = (cons.consolidation_logger.data.loc[:,'ra_category']=='ra_monthly_filing') & \
                (
                    (cons.consolidation_logger.data.loc[:,'status']=='Invalid File') | \
                    (cons.consolidation_logger.data.loc[:,'status']=='File Not Submitted') | \
                    (cons.consolidation_logger.data.loc[:,'status']=='File Not Found')
                )
            missing_lses = ', '.join(cons.consolidation_logger.data.loc[missing_filings,'organization_id'])
            if missing_filings.sum()>1:
                cons.logger.log('{} Monthly Filings Are Not Available for Consolidation: {}'.format(missing_filings.sum(),missing_lses),'WARNING')
            elif missing_filings.sum()>0:
                cons.logger.log('{} Monthly Filing is Not Available for Consolidation: {}'.format(missing_filings.sum(),missing_lses),'WARNING')
            cons.initialize_ra_summary()
            cons.initialize_caiso_cross_check()
            cons.consolidate_allocations()
            cons.consolidate_filings()
            cons.consolidate_supply_plans()
        else:
            missing_files = (cons.consolidation_logger.data.loc[:,'ra_category']!='ra_monthly_filing') & \
                (cons.consolidation_logger.data.loc[:,'status']=='File Not Found')
            missing_ra_categories = ', '.join(cons.consolidation_logger.data.loc[missing_files,'ra_category'])
            cons.logger.log('Files Not Ready for Consolidation: {}'.format(missing_ra_categories),'ERROR')
    else:
        pass

    # copy archive files into a compressed zip file:
    if not skip_organize:
        if not cleared_log:
            org.logger.clear_log()
        org.compress_archive()
        if not skip_notify and not skip_download:
            kw.send_results(ready)
    else:
        pass

if __name__=='__main__':
    ra_filings(
        Path(r'M:\Users\RH2\src\ra_filings\config\ra_filings_config.yaml'),
        use_api=True,
        skip_download=True,
        skip_organize=False,
        skip_consolidate=False,
        skip_notify=True
    )