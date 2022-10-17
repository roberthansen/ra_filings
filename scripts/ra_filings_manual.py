from pathlib import Path
from pandas import Timestamp as ts

from ra_filings import ra_filings

if __name__=='__main__':
    for m in [11]:
        filing_month = ts('2022-{:02d}-01'.format(m))
        download = False
        organize = False
        consolidate = True
        notify = False
        export = False

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