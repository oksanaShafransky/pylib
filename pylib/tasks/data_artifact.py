import logging
from datasource import HDFSDataSource, S3DataSource, DatasourceTypes


SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''


logger = logging.getLogger('data_artifact')


class DataArtifact(object):

    def __init__(self, ti, path, required_size=0, required_marker=True, override_data_sources=None):
        self.raw_path = path
        self.min_required_size = required_size
        self.check_marker = required_marker
        self.ti = ti

        # Decide on datasources
        self.raw_data_sources_list = override_data_sources or self.ti.da_data_sources

        # Create DataSources Hirechy
        self.data_sources = []
        for d in self.raw_data_sources_list:
            d_type = d.get('type')
            if d_type == DatasourceTypes.HDFS.value:
                self.data_sources.append(HDFSDataSource(self.raw_path, self.min_required_size,
                                                        self.check_marker, d.get("name"), d.get("prefix")))
            elif d_type == DatasourceTypes.S3.value:
                self.data_sources.append(S3DataSource(self.raw_path, self.min_required_size,
                                                      self.check_marker, d.get("name"), d.get("prefix")))
            else:
                raise Exception("DataArtifact: unknown data source:%s options - %s"
                                % (d_type, {d.name: d.value for d in DatasourceTypes}))

        # Will be filled by the implementations.
        self.locate_data_source = None

    def get_full_uri(self):
        if not self.locate_data_source:
            raise Exception("DataArtifact - Can't call full uri when data_source is not located.")
        return self.locate_data_source.get_full_uri()
