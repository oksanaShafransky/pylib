__author__ = 'Felix'

from datetime import *
from subprocess import Popen


def list_days(year, month):
    ret = []
    curr_dt = date(year, month, 1)
    while curr_dt.month == month:
        ret += [curr_dt]
        curr_dt += timedelta(1)

    return ret


def day_query(year, month, day):
    return """
        use mobile;
        insert overwrite table daily_app_engagement_estimations partition (year=%(year)02d, month=%(month)2d, day=%(day)2d)
        select mrp.app, mrp.country,
        if (op.app is NULL, mrp.raw_devices, op.raw_devices), if (op.app is NULL, mrp.raw_users, op.raw_users), if (op.app is NULL, mrp.raw_installs, op.raw_installs), if (op.app is NULL, mrp.raw_uninstalls, op.raw_uninstalls),
        if (op.app is NULL, mrp.raw_updates, op.raw_updates), if (op.app is NULL, mrp.raw_sessions, op.raw_sessions), if (op.app is NULL, mrp.raw_usage_time, op.raw_usage_time), if (op.app is NULL, mrp.raw_sessions_squared, op.raw_sessions_squared),
        if (op.app is NULL, mrp.raw_real_sessions_squared, op.raw_real_sessions_squared), if (op.app is NULL, mrp.raw_usage_squared, op.raw_usage_squared), if (op.app is NULL, mrp.raw_is_installed, op.raw_is_installed),
        if (op.app is NULL, mrp.raw_is_uninstalled, op.raw_is_uninstalled), if (op.app is NULL, mrp.raw_qualifying_as_active_users, if(op.usage_time is NULL, op.raw_devices, op.raw_qualifying_as_active_users)),
        if (op.app is NULL, mrp.reach, if(op.usage_time is NULL, op.raw_qualifying_as_active_users, op.reach)), if (op.app is NULL, mrp.active_users, if(op.usage_time is NULL, op.reach, op.active_users)),
        if (op.app is NULL, mrp.sessions, if(op.usage_time is NULL, op.active_users, op.sessions)), if (op.app is NULL, mrp.usage_time, if(op.usage_time is NULL, op.sessions, op.usage_time))
        from
        (select * from backup.daily_app_engagement_estimations where year=%(year)02d and month=%(month)02d and day=%(day)02d) mrp
        left outer join
        (select * from tmp_eng.daily_app_engagement_estimations where year=%(year)02d and month=%(month)02d and day=%(day)02d) op
        on
        (mrp.app = op.app and mrp.country = op.country);
    """ % {'year': year, 'month': month, 'day': day}

if __name__ == '__main__':
    for day in list_days(14, 11):
        all_children = []
        query = day_query(day.year, day.month, day.day)
        all_children += [Popen(['hive', '-e', query])]

    for child in all_children:
        child.wait()
