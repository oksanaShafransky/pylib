import logging
from data_artifact import DataArtifact

import os

SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''

logger = logging.getLogger('data_artifact')

class InputRangedDataArtifact(object):

    def __init__(self, ti, collection_path, dates, suffix_format=DEFAULT_SUFFIX_FORMAT, *args, **kwargs):
        self.collection_path = collection_path
        self.dates = dates
        self.suffix_format = suffix_format
        self.ti = ti
        # Create list of data artifacts
        self.ranged_data_artifact = [
            InputDataArtifact(self.ti, os.path.join(self.collection_path, d.strftime(self.suffix_format)),
                               *args, **kwargs)
            for d in self.dates
        ]

    def assert_input_validity(self, *reporters):
        for da in self.ranged_data_artifact:
            da.assert_input_validity(*reporters)

    def resolved_paths_string(self, item_delimiter=','):
        return item_delimiter.join([da.resolved_path for da in self.ranged_data_artifact])

    def resolved_paths_dates_string(self, date_format='%Y-%m-%d', item_delimiter=';', tuple_delimiter=','):
        tuples = [
            (self.ranged_data_artifact[i].resolved_path, self.dates[i].strftime(date_format))
            for i in range(len(self.dates))
        ]
        return item_delimiter.join([tuple_delimiter.join(tup) for tup in tuples])


class InputDataArtifact(DataArtifact):

    def __init__(self, ti, path, required_size=0, required_marker=True, override_data_sources=None, buffer_percent=None,
                 email_list=[]):
        super(InputDataArtifact, self).__init__(ti, path, required_size, required_marker, override_data_sources,
                                                buffer_percent, email_list)

        # Search in datasource one by one break if we found one.
        for d in self.data_sources:
            # Checking current datasource
            logger.info("Checking datasource: " + repr(d))
            logger.info("InputDataArtifact: Datasource check if dir exists on collection: " + self.raw_path)
            if d.is_dir_exist():
                # From here if something breaks datasource will throw exception
                logger.info("InputDataArtifact: Datasource validate marker, required_marker: " + str(self.check_marker))
                d.assert_marker()
                logger.info("InputDataArtifact: Datasource validate size, required_size: " + str(self.min_required_size))
                d.assert_size()

            if d.is_exist and d.is_marker_validated and d.is_size_validated:
                # We found a datasource
                self.locate_data_source = d
                self.report_lineage('input', self.ti)
                self.locate_data_source.log_success()
                return
            d.log_fail_to_find()

        # If we got here we should fail Data artifact with no collection found
        raise Exception("InputDataArtifact - Couldn't locate collection: %s in any of the datasources" % self.raw_path)

    # This function is deprecated
    def assert_input_validity(self, *reporters):
        logger.warn("This function is deprecated. Input validation is done in the init phase."
                    "Linage is automatically reported to ti. "
                    "Use report_lineage if you have more reporters")

    @property
    def resolved_path(self):
        if self.locate_data_source:
            return self.locate_data_source.resolved_path()
        else:
            raise Exception("InputDataArtifact Failure no datasource located")


if __name__ == '__main__':
    # da = InputDataArtifact('path')
    da = InputDataArtifact('/similargroup/data/android-apps-analytics/daily/extractors/extracted-metric-data/rtype=R1001/year=20/month=11/day=07',
                           required_size=10000, required_marker=True)
    print(da.resolved_path)

