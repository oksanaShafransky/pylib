import logging
import json
from pylib.config.SnowflakeConfig import SnowflakeConfig
from datasource import HDFSDataSource, S3DataSource, DatasourceTypes

import os

SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''


logger = logging.getLogger('data_artifact')

# Extract default data sources, it's here because we want it to run once.
default_data_sources_json = json.loads(SnowflakeConfig().get_service_name(service_name="da-data-sources"))


class DataArtifact(object):

    def __init__(self, path, required_size=0, required_marker=True, override_data_sources=None, ti=None):
        self.raw_path = path
        self.min_required_size = required_size
        self.check_marker = required_marker

        # Decide on datasources - This section still need redesign after we will enable datasource changes through ti.
        self.raw_data_sources_list = override_data_sources if override_data_sources else default_data_sources_json

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
                raise Exception("DataArtifact: unknown data source:%s options - %s" % (d_type, {d.name: d.value for d in DatasourceTypes}))

        # Will be filled by the implementations.
        self.locate_data_source = None

    def get_full_uri(self):
        if not self.locate_data_source:
            raise Exception("DataArtifact - Can't call full uri when data_source is not located.")
        return self.locate_data_source.get_full_uri()



    @property
    def resolved_path(self):
        if self.locate_data_source:
            return self.locate_data_source.resolved_path()
        else:
            raise Exception("DataArtifact Failure no datasource located")