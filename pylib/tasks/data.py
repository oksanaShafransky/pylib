import logging
import json
from pylib.config.SnowflakeConfig import SnowflakeConfig
from datasource import HDFSDataSource, S3DataSource

import os

SUCCESS_MARKER = '_SUCCESS'
DEFAULT_SUFFIX_FORMAT = '''year=%y/month=%m/day=%d'''


logger = logging.getLogger('data_artifact')

# Extract default data sources, it's here because we want it to run once.
default_data_sources_json = json.loads(SnowflakeConfig().get_service_name(service_name="da-input-sources"))


class RangedDataArtifact(object):

    def __init__(self, collection_path, dates, suffix_format=DEFAULT_SUFFIX_FORMAT, *args, **kwargs):
        self.collection_path = collection_path
        self.dates = dates
        self.suffix_format = suffix_format
        # Create list of dataartifacts
        self.ranged_data_artifact = [
            DataArtifact(os.path.join(self.collection_path, d.strftime(self.suffix_format)), *args, **kwargs)
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





class DataArtifact(object):

    def __init__(self, path, required_size=0, required_marker=True, override_data_sources=None, ti=None):
        self.raw_path = path
        self.min_required_size = required_size
        self.check_marker = required_marker

        # Decide on datasources - This section still need redesign after we will enable datasource changes through ti.
        self.raw_data_sources_list = override_data_sources if override_data_sources else default_data_sources_json

        # Create DataSources Hirechy
        self.locate_data_source = None
        data_sources = []
        for d in self.raw_data_sources_list:
            if d.get('type') == "hdfs":
                data_sources.append(HDFSDataSource(self.raw_path, self.min_required_size,
                                                   self.check_marker, d.get("name"), d.get("prefix")))
            elif d.get('type') == "s3":
                data_sources.append(S3DataSource(self.raw_path, self.min_required_size,
                                                   self.check_marker, d.get("name"), d.get("prefix")))
            else:
                raise Exception("DataArtifact: unknown data source")

        #Search in datasource one by one break if we found one.
        for d in data_sources:
            #Checking current datasource
            if d.is_dir_exist():
                #From here if something breaks datasource will throw exception
                d.validate_marker()
                d.validate_size()

            if d.is_exist and d.is_marker and d.is_size:
                #We found a datasource
                self.locate_data_source = d
                return

        #If we got here we should fail Data artifact with no collection found
        raise Exception("DataArtifact - Couldn't locate collection: %s in any of the datasources" % self.raw_path)

    # This function should be depcrecated we only allow it for backward compatibility
    def assert_input_validity(self, *reporters):
        if not self.locate_data_source:
            raise Exception("DataArtifact Failure no valid datasource was found")

        if self.locate_data_source.is_exist and self.locate_data_source.is_size and self.locate_data_source.is_marker:
            for reporter in reporters:
                reporter.report_lineage('input',
                                        {self.locate_data_source.prefixed_collection: self.locate_data_source.effective_size})


    @property
    def resolved_path(self):
        if self.locate_data_source:
            return self.locate_data_source.resolved_path()
        else:
            raise Exception("DataArtifact Failure no datasource located")


if __name__ == '__main__':
    da = DataArtifact('path')
    # da = DataArtifact('/similargroup/data/android-apps-analytics/daily/extractors/extracted-metric-data/rtype=R1001/year=20/month=11/day=07', required_size=1000, required_marker=True)
    # da.assert_input_validity()
    # print(da.resolved_path)

