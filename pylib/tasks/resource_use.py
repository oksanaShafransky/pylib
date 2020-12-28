
from datetime import datetime
from pylib.sw_config.bigdata_kv import get_kv, Purposes
from pylib.config.SnowflakeConfig import SnowflakeConfig


DEFAULT_GB_HR_PRICE = 0.013
DEFAULT_CORE_HR_PRICE = 0.035


# The extra safety layer in the following 2 functions looks weird, but kv may return a legitimate None value if failing

def gb_hour_price():
    kv = get_kv(purpose=Purposes.BigData, snowflake_env=SnowflakeConfig().def_env)
    kv_gb_hour_price = kv.get_or_default('resource_cost/gb_hours', DEFAULT_GB_HR_PRICE)
    return float(kv_gb_hour_price) if kv_gb_hour_price is not None else DEFAULT_GB_HR_PRICE


def core_hour_price():
    kv = get_kv(purpose=Purposes.BigData, snowflake_env=SnowflakeConfig().def_env)
    kv_core_hour_price = kv.get_or_default('resource_cost/core_hours', DEFAULT_CORE_HR_PRICE)
    return float(kv_core_hour_price) if kv_core_hour_price is not None else DEFAULT_CORE_HR_PRICE


class UsedResources(object):
    def __init__(self, gb_hrs=0, core_hrs=0):
        self.gb_hours = gb_hrs
        self.core_hours = core_hrs

    @property
    def dollar_price(self):
        return max(self.gb_hours * gb_hour_price(), self.core_hours * core_hour_price())

    @property
    def cost(self):
        return '%.3f$' % self.dollar_price

    def __add__(self, other):
        return UsedResources(self.gb_hours + other.gb_hours, self.core_hours + other.core_hours)

    def __str__(self):
        return '%.2f GBHours, %.2f CoreHours' % (self.gb_hours, self.core_hours)


def collect_resources(application_stats):
    return UsedResources(
        gb_hrs=application_stats.get('memorySeconds', 0.1) / (1000.0 * 60 * 60),  # transform from MBSeconds
        core_hrs=application_stats.get('vcoreSeconds', 0.1) / (60.0 * 60)  # transform from CoreSeconds
    )


def aggregate_resources(applications):
    ret = UsedResources()
    for app in applications:
        ret += collect_resources(app)

    return ret


def store_resources_used(task_name, resources, start_time=None, end_time=None):
    from pylib.config.SnowflakeConfig import SnowflakeConfig
    print ("store_resources_usedTask name=%s" % task_name)
    task_fields = task_name.split('.')

    dag_id, task_id, execution_id = task_fields[1:4]
    execution_date = execution_id.split('_')[0]
    run_date = datetime.now().strftime('%Y-%m-%d')
    # TODO retrieve connection string from snowflake
    with SnowflakeConfig().get_sql_connection(service_name='bigsize-db') as sql_conn:
        exists = sql_conn.execute("SELECT attempts, gb_hours, core_hours FROM task_resource_usage WHERE tag='%s'" % task_name)
        if exists > 0:
            current = list(sql_conn)[0]
            curr_attempts, curr_mem, curr_cores = current
            updated_attempts, updated_mem, updated_cores = int(curr_attempts) + 1, float(curr_mem) + resources.gb_hours, float(curr_cores) + resources.core_hours
            sql_conn.execute("UPDATE task_resource_usage SET attempts=%d, gb_hours=%.2f, core_hours=%.2f WHERE tag='%s'" % (updated_attempts, updated_mem, updated_cores, task_name))
        else:
            sql_conn.execute("""
                INSERT INTO task_resource_usage (tag, dag_id, task_id, execution_date, run_date, attempts, gb_hours, core_hours, start_time, end_time, estimated_cost) 
                VALUES ('%s', '%s', '%s', '%s', '%s', 1, %.2f, %.2f, '%s', '%s', %.3f)
                """ % (task_name, dag_id, task_id, execution_date, run_date,
                       resources.gb_hours, resources.core_hours,
                       start_time.strftime('%H:%M:%S') if start_time is not None else '00:00:00',
                       end_time.strftime('%H:%M:%S') if end_time is not None else '00:00:00',
                       resources.dollar_price)
                )

