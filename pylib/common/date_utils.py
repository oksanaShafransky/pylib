from datetime import date
from dateutil.relativedelta import relativedelta


def get_dates_range(end, range_len, step_type='days', step_size=1, date_offset=0):
    # type: (date, int, str, int, int) -> List[date]
    """

    :param end:         this param is aimed for dag execution_date. for use cases where we need dates from current
    :param range_len:   list length output dates range
    :param step_type:   days/weeks/months/years (relativedelta API)
    :param step_size:   sets the actual step size, depends on step_type date and back
    :param date_offset: date offset from end date (date_offset == 1 -> end date is excluded from range)
    :return: list of datetime objects for each date in range starting from (end - offset) and going backwards.
             Step interval is step_type * step_size
    """
    truncated_end = date(end.year, end.month, end.day)
    step = relativedelta(**{step_type: step_size})
    range_end = truncated_end - step * date_offset
    range_start = range_end - step * (range_len - 1)
    return get_dates_list(range_start, range_end, step)


def get_dates_list(start, end, step=relativedelta(days=1)):
    # type: (date, date, relativedelta) -> List[date]
    """

    :param start:   Start date for the range
    :param end:     End date for the range
    :param step:    Step size for the range
    :return: list of datetime objects for each date in range starting from (end - offset) and going backwards
    """
    dates_range = []
    curr = start
    delta = step
    assert start <= end, "Start date can't be greater than End date!"
    while curr <= end:
        dates_range.append(curr)
        curr = start + delta
        delta = delta + step
    return dates_range
