"""
    File name: stats.py
    Author: Mayur Sonawane
    Date Created: 11/14/2021
    Date Updated: 11/16/2021
    Python Version: 3.9
    Description: How much revenue is the client getting from external Search Engines,
    such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?
"""
# import sys
from pyspark.sql import SparkSession

from analysis import config as cfg
from analysis.revenue import HitAnalysis
from analysis.utils.logger import Logger

if __name__ == "__main__":
    # Prepare logger to log info, warning, error
    logger = Logger().get_logger(cfg.app_name)

    # Fetch config
    app_name = cfg.app_name  # str(sys.argv)[0]
    input_file_path = cfg.ip_file_path  # str(sys.argv)[1]
    output_file_path = cfg.op_file_path  # str(sys.argv)[2]

    # Create Spark session
    spark = SparkSession.builder.master("local[1]") \
        .appName(app_name) \
        .getOrCreate()
    logger.info("Created Spark Session")
    logger.info("File input path - {}".format(input_file_path))
    # Prepare the Revenue object
    ha = HitAnalysis(spark)
    logger.info("Hit Analysis object created")

    initial_df = ha.read_file(input_file_path)
    logger.info("Initial Dataframe- ")
    logger.info(initial_df)
    # initial_df.show()

    # Calculate revenue logic
    products_df = ha.explode_products_list(initial_df)
    products_total_rev_df = ha.add_total_revenue(products_df)
    purchase_df = ha.get_total_rev_by_ip(products_total_rev_df)
    ext_domain_df = ha.get_ext_search_engine_except_esshopzilla(initial_df)
    domain_rev_df = ha.get_domain_rev(ext_domain_df, purchase_df)
    result_df = ha.get_agg_keywords_rev(domain_rev_df)
    pandas_df = ha.convert_to_pandas(result_df)

    # # df2 = rev.df.select(F.split('product_list', ',').alias('product_list'))
    # df2 = rev.df.withColumn("product_list", F.split('product_list', ','))
    # # df2.show()
    #
    # product_counts = df2.select(F.size('product_list').alias('product_size')).agg(F.max('product_size')).collect()[0][0]
    #
    # for i in range(product_counts):
    #     df2 = df2.withColumn("product_list_{}".format(i), df2['product_list'][i])
    #
    # # df2.show()
    #
    # products = df2.drop("product_list")
    # names = [name for name in products.schema.names if name.startswith("product_list")]
    # for i, name in enumerate(names):
    #     products = (products.withColumn("product_{}_category".format(i), F.split(name, ";").getItem(0))
    #                 .withColumn("product_{}_name".format(i), F.split(name, ";").getItem(1))
    #                 .withColumn("product_{}_#_items".format(i), F.split(name, ";").getItem(2))
    #                 .withColumn("product_{}_total_revenue".format(i), F.split(name, ";").getItem(3))
    #                 .withColumn("product_{}_custom_events".format(i), F.split(name, ";").getItem(4))
    #                 .withColumn("product_{}_merchandizing_evar".format(i), F.split(name, ";").getItem(5))
    #                 ).drop(name)

    # # products.show()
    # products = products.withColumn('total_revenue',
    #                                sum(products[col] for col in products.columns if col.endswith("_total_revenue")))
    # products.show()

    # purchase_df = products.groupBy("ip").agg(F.sum('total_revenue').alias("total_revenue"))
    # purchase_df.show()

    # domainUDF = F.udf(lambda z: HitAnalysis.get_domain(z))
    # keywordUDF = F.udf(lambda z: HitAnalysis.get_keyword(z))
    #
    # # Get distinct External Search Engine, other than esshopzilla
    # domain_df = initial_df.select("ip", domainUDF("referrer").alias("Search Engine Domain"),
    #                           keywordUDF("referrer").alias("Keyword")).distinct()
    # ext_domain_df = domain_df.filter(domain_df["Search Engine Domain"] != "Esshopzilla")
    # # domain_df.show()
    # # ext_domain_df.show()

    # domain_rev_df = ext_domain_df.join(purchase_df, ext_domain_df.ip == purchase_df.ip, "inner") \
    #     .select(ext_domain_df["Search Engine Domain"], ext_domain_df["Keyword"], purchase_df["total_revenue"])
    # domain_rev_df.show()

    # result_df = domain_rev_df.groupBy("Search Engine Domain") \
    #     .agg(
    #     F.concat_ws(" ", F.collect_set(domain_rev_df["Keyword"])).alias("Search Keyword"),
    #     F.sum("total_revenue").alias("total_revenue")
    # ) \
    #     .orderBy("total_revenue", ascending=False)

    # # Converting PySpark Dataframe to Pandas dataframe
    # pandas_df = result_df.toPandas()

    # Write output to file
    ha.write_df(pandas_df, output_file_path)

    logger.info("Output written to file path at - {}".format(output_file_path))
