from pathlib import Path
from pandas import Timestamp as ts

from ra_filings import ra_filings

if __name__=='__main__':
    for m in [6]:
        filing_month = ts(2024,m,1)
        download = False
        organize = True
        consolidate = True
        notify = True
        export = False

        if any([download,organize,consolidate,notify,export]):
            ra_filings(
                configuration_options_path=Path(r'\\Sf150pyclfs26\PYCLIENTFS\Users\svc_energyRA\ra_filings\config\ra_filings_config_{}.yaml'.format(filing_month.year)),
                download=download,
                organize=organize,
                consolidate=consolidate,
                notify=notify,
                export=export,
                filing_month=filing_month
            )
        else:
            pass