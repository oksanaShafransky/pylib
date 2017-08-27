from datetime import date
from dateutil.relativedelta import relativedelta


def get_dates_range(end, num_leaps, leap_interval='days', date_offset=0):
    """
    returns list of datetime objects for each date in range starting from (end - offset) and going backwards.

    :param end: this param is aimed for dag execution_date. for use cases where we need dates from current date and back
    :param leap_interval: jumps interval size ('years', 'months', 'weeks', 'days')
    :param num_leaps: number of dates in range
    :param date_offset: date offset from end date (date_offset == 1 -> end date is excluded from range
    """
    truncated_end = date(end.year, end.month, end.day)
    dates_range = []
    delta = relativedelta()
    for i in range(date_offset, num_leaps + date_offset):
        setattr(delta, leap_interval, i)
        dates_range.append(truncated_end - delta)
    return dates_range
