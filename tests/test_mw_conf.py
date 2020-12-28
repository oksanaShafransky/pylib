from unittest import TestCase

from sw_jobs.mw_conf import MobileWebConfig


class TestMobileWebConfig(TestCase):
    def test_countries(self):
        # given a country numeric code :840
        # assert that the result is US
        input  = '840'
        output = MobileWebConfig().countries.get(input, None)
        assert output == 'US'
