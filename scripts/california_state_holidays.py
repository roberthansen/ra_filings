from pandas import Timestamp as ts,Timedelta as td

def california_state_holidays(year):
    '''
    provides a dictionary containing pandas timestamps for the all california state holidays as observed in a given year.

    parameters:
        year - an integer representing a year.
    '''
    holidays = {
        'new_years' :  ts(year,1,1),
        'mlkjr' : ts(year,1,1)+td(days=20-ts(year,1,7).weekday()),
        'presidents' : ts(year,2,1)+td(days=20-ts(year,2,7).weekday()),
        'cesar_chavez' : ts(year,3,31),
        'memorial' : ts(year,5,31)+td(days=-ts(year,5,31).weekday()),
        'independence' : ts(year,7,4),
        'labor' : ts(year,9,1)+td(days=6-ts(year,9,7).weekday()),
        'veterans' : ts(year,11,11)+ \
            td(days=0+\
                (1 if ts(year,11,11).weekday()==6 else 0)- \
                (1 if ts(year,11,11).weekday()==5 else 0)
            ),
        'thanksgiving' : ts(year,11,1)+ \
            td(days=27-(ts(year,11,7).weekday()-3)%7),
        'day_after_thanksgiving' : ts(year,11,1)+ \
            td(days=28-(ts(year,11,7).weekday()-3)%7),
        'christmas' : ts(year,12,25)+ \
            td(days=0+\
                (1 if ts(year,12,25).weekday()==6 else 0)+ \
                (2 if ts(year,12,25).weekday()==5 else 0)
            ),
    }
    return holidays