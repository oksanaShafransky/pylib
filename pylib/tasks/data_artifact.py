
from datasource import HDFSDataSource, S3DataSource, DatasourceTypes


SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''

class DataArtifact(object):

    def __init__(self, ti, path, required_size=0, required_marker=True, override_data_sources=None,
                 buffer_percent=None, email_list=[]):
        self.raw_path = path
        self.check_marker = required_marker
        self.ti = ti

        self.email_list = email_list or self.ti.email_list
        self.original_required_size = required_size

        # Decide on buffer size
        self.buffer_size = self.get_buffer_size(float(buffer_percent or self.ti.buffer_percent))

        self.min_required_size = self.set_size_check_threshold(required_size)

        # Decide on datasources
        self.raw_data_sources_list = override_data_sources or self.ti.da_data_sources

        # Create DataSources Hirechy
        self.data_sources = []
        for d in self.raw_data_sources_list:
            d_type = d.get('type')
            if d_type == DatasourceTypes.HDFS.value:
                self.data_sources.append(HDFSDataSource(self.raw_path, self.min_required_size,
                                                        self.check_marker, d.get("name"), d.get("prefix"),
                                                        self.original_required_size, self.email_list))
            elif d_type == DatasourceTypes.S3.value:
                self.data_sources.append(S3DataSource(self.raw_path, self.min_required_size,
                                                      self.check_marker, d.get("name"), d.get("prefix"),
                                                      self.original_required_size, self.email_list))
            else:
                raise Exception("DataArtifact: unknown data source:%s options - %s"
                                % (d_type, {d.name: d.value for d in DatasourceTypes}))

        # Will be filled by the implementations.
        self.locate_data_source = None

    def get_full_uri(self):
        if not self.locate_data_source:
            raise Exception("DataArtifact - Can't call full uri when data_source is not located.")
        return self.locate_data_source.get_full_uri()

    def report_lineage(self, report_type, *reporters):
        for reporter in reporters:
            reporter.log_lineage_hdfs(direction=report_type, directories=[self.locate_data_source.prefixed_collection])

    def set_size_check_threshold(self, required_size):
        if self.buffer_size < 0 or self.buffer_size > 1:
            raise Exception("DataArtifact - buffer percent must between 0 and 100")
        else:
            return required_size * (1 - self.buffer_size)

    def get_size_check_threshold(self):
        return self.min_required_size

    @staticmethod
    def get_buffer_size(buffer_size):
        return buffer_size / 100.0
