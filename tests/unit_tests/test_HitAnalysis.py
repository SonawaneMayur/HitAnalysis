import os.path
import unittest
from datetime import datetime as dt

import pandas as pd
from pyspark.sql import SparkSession

from analysis import config as cfg
from analysis.revenue import HitAnalysis


class MyTestCase(unittest.TestCase):
    spark = SparkSession.builder.master("local[1]") \
        .appName(cfg.app_name) \
        .getOrCreate()

    def test_read_file(self):
        ha = HitAnalysis(self.spark)
        initial_df = ha.read_file(
            "/Users/mayursonawane/PycharmProjects/hit_analysis/tests/unit_tests/data/input/data.tsv")
        self.assertIsNotNone(initial_df, "Can not read file")
        # self.assertEqual(True, False)  # add assertion here

    def test_get_domain(self):
        url = "http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%" \
              "3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi="
        domain_name = HitAnalysis.get_domain(url)
        self.assertEqual(domain_name, "Google", "Not able to fetch domain from url")

    def test_get_keyword(self):
        url = "http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aoff" \
              "icial&hs=ZzP&q=Ipod&aq=f&oq=&aqi="
        search_keyword = HitAnalysis.get_keyword(url)
        self.assertEqual(search_keyword, "Ipod", "Not able to fetch search keyword from url")

    def test_get_date(self):
        expected_date = dt.now().strftime("%Y-%m-%d")
        current_date = HitAnalysis.get_date()
        self.assertEqual(current_date, expected_date, "Date format doesn't match")

    def test_write_df(self):
        data = [[1, 2, 3], [3, 4, 5]]
        df = pd.DataFrame(data, columns=['A', 'B', 'C'])
        ha = HitAnalysis(self.spark)
        expected_date = dt.now().strftime("%Y-%m-%d")

        ha.write_df(df, "/Users/mayursonawane/PycharmProjects/hit_analysis/tests/unit_tests/data/output")

        expected_path = "/Users/mayursonawane/PycharmProjects/hit_analysis/tests/unit_tests/data/output" \
                        "/{}_SearchKeywordPerformance.tab".format(expected_date)
        self.assertEqual(os.path.exists(expected_path), True, "File failed to write at path")


if __name__ == '__main__':
    unittest.main()
