import logging
import json
from pylib.config.SnowflakeConfig import SnowflakeConfig
from datasource import HDFSDataSource, S3DataSource, DatasourceTypes
from data_artifact import DataArtifact

import os

SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''


logger = logging.getLogger('data_artifact')


class OutputDataArtifact(DataArtifact):

    def __init__(self, path, required_size=0, required_marker=True, override_data_sources=None, ti=None):
        super(OutputDataArtifact, self).__init__(path, required_size, required_marker, override_data_sources, ti)
        # Take first data_source available from the list. This is our output data source
        self.locate_data_source = self.data_sources[0]

    def assert_output_validity(self, *reporters):
        if not self.locate_data_source:
            raise Exception("InputDataArtifact Failure no valid datasource was found")
        # Checking current datasource
        logger.info("Checking datasource: " + repr(self.locate_data_source))
        logger.info("OutputDataArtifact: Datasource check if dir exsits on collection: " + self.raw_path)
        if self.locate_data_source.is_dir_exist():
            # From here if something breaks datasource will throw exception
            logger.info("OutputDataArtifact: Datasource validate marker, required_marker: " + str(self.check_marker))
            self.locate_data_source.assert_marker()
            logger.info("OutputDataArtifact: Datasource validate size, required_size: " + str(self.min_required_size))
            self.locate_data_source.assert_size()

        if self.locate_data_source.is_exist and self.locate_data_source.is_marker_validated and self.locate_data_source.is_size_validated:
            # We found a datasource
            self.locate_data_source.log_success()
            return

        self.locate_data_source.log_fail_to_find()

        if self.locate_data_source.is_exist and self.locate_data_source.is_size_validated and self.locate_data_source.is_marker_validated:
            for reporter in reporters:
                reporter.report_lineage('output',
                                        {self.locate_data_source.get_full_uri(): self.locate_data_source.effective_size})


if __name__ == '__main__':
    # da = InputDataArtifact('path')
    da = OutputDataArtifact('/similargroup/data/android-apps-analytics/daily/extractors/extracted-metric-data/rtype=R1001/year=20/month=11/day=30', required_size=10000, required_marker=True)
    print(da.get_full_uri())
    da.assert_output_validity()
    print(da.get_full_uri())


