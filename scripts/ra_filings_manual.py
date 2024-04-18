from pathlib import Path
from pandas import Timestamp as ts

from ra_filings import ra_filings

#changed line 19 to point to ra_filings_config_2023.yaml

if __name__=='__main__':
    for m in [1]:
        filing_month = ts(2024,m,1)
        download = False
        organize = True
        consolidate = True
        notify = False
        export = False

        if any([download,organize,consolidate,notify,export]):
            ra_filings(
                configuration_options_path=Path(r'\\Sf150pyclfs26\PYCLIENTFS\Users\svc_energyRA\ra_filings_development\ra_filings\config\ra_filings_config.yaml'),
                download=download,
                organize=organize,
                consolidate=consolidate,
                notify=notify,
                export=export,
                filing_month=filing_month
            )
        else:
            pass