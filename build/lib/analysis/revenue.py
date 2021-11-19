"""
    File name: revenue.py
    Author: Mayur Sonawane
    Date Created: 11/14/2021
    Date Updated: 11/16/2021
    Python Version: 3.9
    Description: This class contains the supportive class and their function to get Revenue
"""
import re
from datetime import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType, StringType

class HitAnalysis:
    # The expected schema for Hit Analysis data
    SCHEMA = StructType([
        StructField("hit_time_gmt", IntegerType()),
        StructField("date_time", TimestampType()),
        StructField("user_agent", StringType()),
        StructField("ip", StringType()),
        StructField("event_list", StringType()),
        StructField("geo_city", StringType()),
        StructField("geo_region", StringType()),
        StructField("geo_country", StringType()),
        StructField("pagename", StringType()),
        StructField("page_url", StringType()),
        StructField("product_list", StringType()),
        StructField("referrer", StringType())
    ])

    def __init__(self, spark):
        self.spark = spark
        self.domainUDF = f.udf(lambda z: HitAnalysis.get_domain(z))
        self.keywordUDF = f.udf(lambda z: HitAnalysis.get_keyword(z))

    @staticmethod
    def get_domain(url):
        m = re.search('https?://www.([A-Za-z_0-9.-]+).com.*', url)
        try:
            return str(m.group(1)).capitalize()
        except:
            None

    @staticmethod
    def get_keyword(url):
        m = re.search('.*[?&]q=([A-Za-z_0-9.-]+)&.*', url)
        try:
            return str(m.group(1)).capitalize()
        except:
            None

    @staticmethod
    def get_date(date_format="%Y-%m-%d"):
        return dt.now().strftime(date_format)

    def get_file_name(self):
        return "{}_SearchKeywordPerformance.tab".format(HitAnalysis.get_date())

    def read_file(self, file_path):
        try:
            return self.spark.read.csv(file_path, sep=r'\t', header=True, schema=HitAnalysis.SCHEMA)
        except Exception:
            raise

    def convert_to_pandas(self, df):
        return df.toPandas()

    def write_df(self, df, file_path):
        try:
            if isinstance(df, DataFrame):
                df = df.toPandas()
                # df.repartition(1).write.csv(path='{}/{}'.format(file_path, self.get_file_name()), mode='overwrite',
                #                             header=True, sep='\t')

            df.to_csv('{}/{}'.format(file_path, self.get_file_name()), sep='\t', index=False)
        except Exception:
            raise

    def explode_products_list(self, df):
        df2 = df.withColumn("product_list", f.split('product_list', ','))
        # df2.show()

        product_counts = \
            df2.select(f.size('product_list').alias('product_size')).agg(f.max('product_size')).collect()[0][0]

        for i in range(product_counts):
            df2 = df2.withColumn("product_list_{}".format(i), df2['product_list'][i])

        # df2.show()

        products = df2.drop("product_list")
        names = [name for name in products.schema.names if name.startswith("product_list")]
        for i, name in enumerate(names):
            products = (products.withColumn("product_{}_category".format(i), f.split(name, ";").getItem(0))
                        .withColumn("product_{}_name".format(i), f.split(name, ";").getItem(1))
                        .withColumn("product_{}_#_items".format(i), f.split(name, ";").getItem(2))
                        .withColumn("product_{}_total_revenue".format(i), f.split(name, ";").getItem(3))
                        .withColumn("product_{}_custom_events".format(i), f.split(name, ";").getItem(4))
                        .withColumn("product_{}_merchandizing_evar".format(i), f.split(name, ";").getItem(5))
                        ).drop(name)
        return products

    def add_total_revenue(self, df):
        return df.withColumn('total_revenue', sum(df[col] for col in df.columns if col.endswith("_total_revenue")))

    def get_total_rev_by_ip(self, df):
        return df.groupBy("ip").agg(f.sum('total_revenue').alias("total_revenue"))

    def get_ext_search_engine_except_esshopzilla(self, initial_df):
        # Get distinct External Search Engine, other than esshopzilla
        domain_df = initial_df.select("ip", self.domainUDF("referrer").alias("Search Engine Domain"),
                                      self.keywordUDF("referrer").alias("Keyword")).distinct()
        return domain_df.filter(domain_df["Search Engine Domain"] != "Esshopzilla")

    def get_domain_rev(self, ext_domain_df, purchase_df):
        return ext_domain_df.join(purchase_df, ext_domain_df.ip == purchase_df.ip, "inner") \
            .select(ext_domain_df["Search Engine Domain"], ext_domain_df["Keyword"], purchase_df["total_revenue"])

    def get_agg_keywords_rev(self, df):
        return df.groupBy("Search Engine Domain") \
            .agg(
            f.concat_ws(" ", f.collect_set(df["Keyword"])).alias("Search Keyword"),
            f.sum("total_revenue").alias("Revenue")
        ) \
            .orderBy("Revenue", ascending=False)
