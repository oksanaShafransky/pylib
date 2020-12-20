import logging
from data_artifact import DataArtifact


SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''


logger = logging.getLogger('data_artifact')


class OutputDataArtifact(DataArtifact):

    def __init__(self, ti, path, required_size=0, required_marker=True, override_data_sources=None):
        super(OutputDataArtifact, self).__init__(ti, path,required_size, required_marker, override_data_sources)
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

    @property
    def resolved_path(self):
        if self.locate_data_source:
            return self.locate_data_source.get_full_uri()
        else:
            raise Exception("OutputDataArtifact Failure no datasource located")


if __name__ == '__main__':
    # da = InputDataArtifact('path')
    override = [{"type": "s3", "name": "similargroup-backup-retention", "prefix": "/mrp"}]
    da = OutputDataArtifact('/similargroup/data/android-apps-analytics/daily/extractors/extracted-metric-data/rtype=R1001/year=20/month=11/day=30',
                            required_size=10000, required_marker=True, override_data_sources=override)
    print(da.resolved_path)
    da.assert_output_validity()
    print(da.resolved_path)


