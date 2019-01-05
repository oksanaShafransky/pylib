class UsedResources(object):
    def __init__(self, gb_hrs=0, core_hrs=0):
        self.gb_hours = gb_hrs
        self.core_hours = core_hrs

    def __add__(self, other):
        return UsedResources(self.gb_hours + other.gb_hours, self.core_hours + other.core_hours)

    def __str__(self):
        return '%.2f GBHours, %.2f CoreHours' % (self.gb_hours, self.core_hours)


def collect_resources(application_stats):
    return UsedResources(
        gb_hrs=application_stats.get('memorySeconds', 0.1) / (1000.0 * 60 * 60),  # transform from MBSeconds
        core_hrs=application_stats.get('vcoreSeconds', 0.1) / (60.0 * 60)  # transform from CoreSeconds
    )
