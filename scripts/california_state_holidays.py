from pandas import Timestamp as ts,Timedelta as td

def california_state_holidays(year):
    '''
    provides a dictionary containing pandas timestamps for the all california state holidays as observed in a given year.

    parameters:
        year - an integer representing a year.
    '''
    holidays = {
        'new_years' :  ts('{}-01-01'.format(year)),
        'mlkjr' : ts('{}-01-01'.format(year))+td(days=20-ts('{}-01-07'.format(year)).weekday()),
        'presidents' : ts('{}-02-01'.format(year))+td(days=20-ts('{}-02-07'.format(year)).weekday()),
        'cesar_chavez' : ts('{}-03-31'.format(year)),
        'memorial' : ts('{}-05-31'.format(year))+td(days=-ts('{}-05-31'.format(year)).weekday()),
        'independence' : ts('{}-07-04'.format(year)),
        'labor' : ts('{}-09-01'.format(year))+td(days=6-ts('{}-09-07'.format(year)).weekday()),
        'veterans' : ts('{}-11-11'.format(year))+ \
            td(days=0+\
                (ts('{}-11-11'.format(year)).weekday()==6)- \
                (ts('{}-11-11'.format(year)).weekday()==5)
            ),
        'thanksgiving' : ts('{}-11-01'.format(year))+td(days=27-(ts('{}-11-07'.format(year)).weekday()-3)%7),
        'day_after_thanksgiving' : ts('{}-11-01'.format(year))+td(days=28-(ts('{}-11-07'.format(year)).weekday()-3)%7),
        'christmas' : ts('{}-12-25'.format(year))+ \
            td(days=0+\
                (ts('{}-12-25'.format(year)).weekday()==6)+ \
                2*(ts('{}-12-25'.format(year)).weekday()==5)
            ),
    }
    return holidays