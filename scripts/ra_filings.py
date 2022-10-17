import sys
from pathlib import Path
from pandas import Timestamp as ts,Timedelta as td

from ra_consolidator import WorkbookConsolidator
from login import kw_user,kw_api_client
from ra_organizer import Organizer
from export_to_ezdb import DataExporter

# 2021-12-09
# California Public Utilities Commission
# Robert Hansen, PE

# download and organize resource adequacy monthly/annual reports
def ra_filings(configuration_options_path:Path,download:bool=False,organize:bool=False,consolidate:bool=False,notify:bool=False,export:bool=False,filing_month:ts=None):
    '''
    this function is the primary means of interacting with the resource
    adequacy monthly filing compliance tool. It can be run as a scheduled task
    on a daily basis.
    '''
    cleared_log = False

    # download all new attachments from past month of emails in kiteworks:
    if download or notify:
        from kiteworks_api_downloader import AttachmentDownloader
        kw = AttachmentDownloader(configuration_options_path=configuration_options_path,user=kw_user,api_client=kw_api_client,filing_month=filing_month)
        kw.logger.clear_log()
        if download:
            kw.download_current_month()
        else:
            pass
    else:
        pass

    # organize downloaded attachments into final report directory:
    org = Organizer(configuration_options_path,filing_month=filing_month)
    if organize:
        if not cleared_log:
            org.logger.clear_log
        org.organize()
    else:
        pass

    # consolidate data from filings and requirement tables:
    if consolidate:
        cons = WorkbookConsolidator(configuration_options_path,filing_month=filing_month)
        if not cleared_log:
            cons.logger.clear_log()
        cons.consolidation_logger.clear_log()
        cons.consolidation_logger.commit()
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
            completed = True
        else:
            missing_files = (cons.consolidation_logger.data.loc[:,'ra_category']!='ra_monthly_filing') & \
                (cons.consolidation_logger.data.loc[:,'status']=='File Not Found')
            missing_ra_categories = ', '.join(cons.consolidation_logger.data.loc[missing_files,'ra_category'])
            cons.logger.log('Files Not Ready for Consolidation: {}'.format(missing_ra_categories),'ERROR')
            completed = False
    else:
        pass

    # copy archive files into a compressed zip file:
    if organize or consolidate:
        if not cleared_log:
            org.logger.clear_log()
        org.compress_archive()
        if consolidate and notify:
            kw.send_results(completed)
    else:
        pass

    # export input data and results for upload to ezdb:
    if export:
        data_exporter = DataExporter(configuration_options_path,filing_month=filing_month)
        data_exporter.write_all()
        data_exporter.update_master_lookup_table()

    else:
        pass

if __name__=='__main__':
    today = ts.now().replace(hour=0,minute=0,second=0,microsecond=0)

    argv = sys.argv
    daily='--daily' in argv or '-D' in argv

    # run daily schedule check, ignore all other arguments:
    if daily:
        print('running daily ...')
        if (today+td(days=45)).day==1:
            print('downloading (T-45) ...')
            # download only:
            filing_month = (today + td(days=45))
            download = True
            organize = False
            consolidate = False
            notify = False
            export = False
        elif (today+td(days=44)).day==1:
            print('organizing (T-44)')
            # organize and notify:
            filing_month = (today + td(days=44))
            download = False
            organize = True
            consolidate = False
            notify = True
            export = False
        elif (today+td(days=43)).day==1:
            print('performing compliance check (T-43)')
            # perform compliance check and send summaries:
            filing_month = (today + td(days=43))
            download = True
            organize = True
            consolidate = True
            notify = True
            export = False
        elif (today+td(days=28)).day==1:
            print('downloading updates and re-running compliance (T-28)')
            # download revised filings and re-run compliance check:
            filing_month = (today + td(days=28))
            download = True
            organize = True
            consolidate = True
            notify = True
            export = False
        elif (today+td(days=25)).day==1:
            print('exporting to ezdb (T-25)')
            # export results for ezdb:
            filing_month = (today + td(days=25))
            download = False
            organize = False
            consolidate = False
            notify = False
            export = True
        else:
            print('no daily tasks scheduled')
            # do nothing:
            download = False
            organize = False
            consolidate = False
            notify = False
            export = False
    else:
        filing_month = (today + td(days=50)).replace(day=1)
        download='--download' in argv or '-d' in argv
        organize='--organize' in argv or '-o' in argv
        consolidate='--consolidate' in argv or '-c' in argv
        notify='--notify' in argv or '-n' in argv
        export='--export' in argv or '-e' in argv

    if any([download,organize,consolidate,notify,export]):
        ra_filings(
            Path(r'\\Sf150pyclfs26\PYCLIENTFS\Users\svc_energyRA\ra_filings\config\ra_filings_config.yaml'),
            download=download,
            organize=organize,
            consolidate=consolidate,
            notify=notify,
            export=export,
            filing_month=filing_month
        )
    else:
        pass