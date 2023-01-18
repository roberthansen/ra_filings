from pathlib import Path
from pandas import Timestamp as ts

from ra_filings import ra_filings

#changed line 19 to point to ra_filings_config_2023.yaml

if __name__=='__main__':
    for m in [3]:
        filing_month = ts('2023-{:02d}-01'.format(m))
        download = False
        organize = False
        consolidate = True
        notify = False #set this to false (NP)
        export = False

        if any([download,organize,consolidate,notify,export]):
            ra_filings(
                configuration_options_path=Path(r'\\Sf150pyclfs26\PYCLIENTFS\Users\svc_energyRA\ra_filings\config\ra_filings_config_2023.yaml'),
                download=download,
                organize=organize,
                consolidate=consolidate,
                notify=notify,
                export=export,
                filing_month=filing_month
            )
        else:
            pass