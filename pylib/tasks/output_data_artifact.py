import logging
from data_artifact import DataArtifact


SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''

logger = logging.getLogger('data_artifact')


class OutputDataArtifact(DataArtifact):

    def __init__(self, ti, path, required_size=0, required_marker=True, override_data_sources=None, buffer_percent=None,
                 email_list=""):
        super(OutputDataArtifact, self).__init__(ti, path, required_size, required_marker, override_data_sources,
                                                 buffer_percent, email_list)
        # Take first data_source available from the list. This is our output data source
        self.locate_data_source = self.data_sources[0]

    def assert_output_validity(self, *reporters):
        if not self.locate_data_source:
            raise Exception("InputDataArtifact Failure no valid datasource was found")
        # Checking current datasource
        logger.info("Checking datasource: " + repr(self.locate_data_source))
        logger.info("OutputDataArtifact: Datasource check if dir exists on collection: " + self.raw_path)
        if self.locate_data_source.is_dir_exist():
            if self.ti.ignore_marker_check is True:
                logger.info("OutputDataArtifact: Ignoring datasource validate marker")
                self.locate_data_source.is_marker_validated = True
            else:
                # From here if something breaks datasource will throw exception
                logger.info("OutputDataArtifact: Datasource validate marker, required_marker: " + str(self.check_marker))
                self.locate_data_source.assert_marker()
            if self.ti.ignore_size_check is True:
                logger.info("OutputDataArtifact: Ignoring datasource validate size")
                self.locate_data_source.is_size_validated = True
            else:
                logger.info("OutputDataArtifact: Datasource validate size, required_size: " + str(self.min_required_size))
                self.locate_data_source.assert_size()

        if self.locate_data_source.is_exist and self.locate_data_source.is_marker_validated and self.locate_data_source.is_size_validated:
            # We found a datasource
            self.report_lineage('output', self.ti)
            if len(reporters) > 0:
                logger.warn("The usage of reporters is no longer supported. "
                            "linage is automatically reported to ti. "
                            "use report_lineage if you have more reporters")
            self.locate_data_source.log_success()
            return

        self.locate_data_source.log_fail_to_find()

    @property
    def resolved_path(self):
        if self.locate_data_source:
            return self.locate_data_source.get_full_uri()
        else:
            raise Exception("OutputDataArtifact Failure no datasource located")


if __name__ == '__main__':
    # da = InputDataArtifact('path')
    override = [{"type": "s3a", "name": "similargroup-backup-retention", "prefix": "/mrp"}]
    da = OutputDataArtifact('/similargroup/data/android-apps-analytics/daily/extractors/extracted-metric-data/rtype=R1001/year=20/month=11/day=30',
                            required_size=10000, required_marker=True, override_data_sources=override)
    print(da.resolved_path)
    da.assert_output_validity()
    print(da.resolved_path)


