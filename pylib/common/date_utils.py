from datetime import date
from dateutil.relativedelta import relativedelta


def get_dates_range(end, num_leaps, leap_interval='days'):
    """
    returns list of datetime objects for each day in range starting from end and going backwards.
    end date is included

    :param end: this param is aimed for dag execution_date. for use cases where we need dates from current date and back
    :param leap_interval: jumps interval size ('years', 'months', 'weeks', 'days')
    :param num_leaps: number of dates in range
    """
    truncated_end = date(end.year, end.month, end.day)
    dates_range = []
    delta = relativedelta()
    for i in range(0, num_leaps):
        setattr(delta, leap_interval, i)
        dates_range.append(truncated_end - delta)
    return dates_range
