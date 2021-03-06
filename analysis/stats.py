"""
    File name: stats.py
    Author: Mayur Sonawane
    Date Created: 11/14/2021
    Date Updated: 11/16/2021
    Python Version: 3.9
    Description: How much revenue is the client getting from external Search Engines,
    such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?
"""
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from analysis import config as cfg
from analysis.revenue import Revenue
from analysis.utils.logger import Logger


class Stats:

    def launch(self, input_file_path: str) -> object:
        logger = Logger().get_logger(cfg.app_name)
        try:
            # Fetch config
            app_name = cfg.app_name
            #[TODO] - This config we can get as a sys arguments
            # input_file_path = cfg.ip_file_path  # str(sys.argv)[1]
            output_file_path = cfg.op_file_path  # str(sys.argv)[3]

            # Create Spark session
            spark = SparkSession.builder.master(cfg.master_url) \
                .appName(app_name) \
                .getOrCreate()
            logger.info("Created Spark Session")
            logger.info("File input path - {}".format(input_file_path))
            # Prepare the Revenue object
            rev = Revenue(spark)
            logger.info("Hit Analysis object created")

            initial_df = rev.read_file(input_file_path)
            logger.info("Initial Dataframe- ")
            logger.info(initial_df)
            # initial_df.show()

            # Calculate revenue logic
            # Explode product_list
            products_df, products_count = rev.explode_products_list(initial_df)

            # Adding Total revenue column
            products_total_rev_df = rev.add_total_revenue(products_df)
            products_total_rev_df.show(22)

            # Calculate total revenue by ip
            purchase_df = rev.get_total_rev_by_ip(products_total_rev_df)
            purchase_df.show()

            # Get external search engine except eshopzill
            ext_domain_df = rev.get_ext_search_engine_except_esshopzilla(initial_df)

            # Get total revenue by domain
            domain_rev_df = rev.get_domain_rev(ext_domain_df, purchase_df)

            # Get Search Query keywords and revenue by domain
            result_df = rev.get_agg_keywords_rev(domain_rev_df)
            result_df.show()

            # rev.write_df(result_df, output_file_path)
            rev.spark_to_csv(result_df, output_file_path)

            # # To write Spark Dataframe as .tsv file, first convert it into Pandas dataframe
            # pandas_df = ha.convert_to_pandas(result_df)
            #
            # # Write Pandas Dataframe result to output file path
            # ha.write_df(pandas_df, output_file_path)

            logger.info("Output written to file path at - {}".format(output_file_path))

            products_total_rev_df.createOrReplaceTempView("HIT")

            # Busy time of the day
            print("===========Busy time of the day====================")
            busy_df = spark.sql("""
                                    SELECT 
                                        DATE(date_time) as Date, 
                                        HOUR(date_time) as Hour, 
                                        COUNT(ip) as Page_Hits, 
                                        SUM(total_revenue) as Revenue_Generated
                                    FROM HIT
                                    GROUP BY Date, Hour
                                    ORDER BY Date, Hour, Page_Hits desc
                                    """)
            busy_df.show()
            logger.info("Busy time of the day \n {}".format(busy_df.toPandas()))

            # Most visited pages
            print("=============Most visited pages====================")
            visited_df = spark.sql("""
                        SELECT pagename as Page_Name, COUNT(pagename) as Hits 
                        FROM HIT 
                        GROUP BY Page_Name 
                        ORDER BY Hits desc
                        """)
            visited_df.show()
            logger.info("Most visited pages \n {}".format(visited_df.toPandas()))

            # Total Revenue by Country, Region, City
            print("=============Total Revenue and Number of customer's by Country, Region, City====================")
            rev_by_df = spark.sql("""
                        SELECT 
                            geo_country as Country, 
                            geo_region as Region, 
                            geo_city as City, 
                            SUM(coalesce(total_revenue, 0)) as Total_Revenue,
                            COUNT(distinct(ip)) as Number_of_Customer
                        FROM HIT 
                            GROUP BY Country, Region, City
                            ORDER BY Total_Revenue desc
                            """)
            rev_by_df.show()
            logger.info(
                "Total Revenue and Number of customer's by Country, Region, City\n {}".format(rev_by_df.toPandas()))

            # Potential Buyers
            print(
                "=============Potential Buyer's ip, who viewed product or added product into cart====================")
            potential_df = spark.sql("""
                                    SELECT 
                                        distinct ip as Potential_Buyer_ip
                                    FROM HIT 
                                    WHERE
                                        event_list in (2, 12)
                                        AND
                                        event_list not in (
                                                            SELECT DISTINCT ip
                                                            FROM HIT
                                                            WHERE event_list = '1')
                                        """)
            potential_df.show()
            logger.info("Potential Buyer's ip, who viewed product or added product into cart\n {}".format(
                potential_df.toPandas()))

            # Number of customer's by Region
            print(
                "=============Number of customer's by Country, Region, City====================")
            cust_df = spark.sql("""
                      SELECT 
                        geo_country as Country, 
                        geo_region as Region, 
                        geo_city as City, 
                        COUNT(distinct(ip)) as Number_of_Customer
                    FROM HIT 
                        GROUP BY Country, Region, City
                        ORDER BY Number_of_Customer desc
                        """)
            cust_df.show()
            logger.info("Number of customer's by Country, Region, City \n {}".format(cust_df.toPandas()))

            # Total Revenue by product category
            print("=============Total Revenue by product category====================")
            product_info_df = None
            for cnt in range(0, products_count):
                products_cat_cols = [s for s in products_total_rev_df.columns if 'product_{}_category'.format(cnt) in s]+['total_revenue']

                prod_df = products_total_rev_df.select(*products_cat_cols)\
                    .withColumnRenamed(products_cat_cols[0],"Product_Category")\
                    .withColumnRenamed(products_cat_cols[1],"Total_Revenue")
                if not product_info_df:
                    schema_json = prod_df.schema.json()
                    new_schema = StructType.fromJson(json.loads(schema_json))
                    product_info_df = spark.createDataFrame([], new_schema)
                product_info_df = product_info_df.union(prod_df)
            product_info_df.show()
            product_info_df.createOrReplaceTempView("PRODUCT_INFO")
            rev_by_cat_df = spark.sql("""
                                SELECT
                                    distinct(Product_Category) as Product_Category,
                                    SUM(coalesce(Total_Revenue, 0)) as Total_Revenue
                                FROM PRODUCT_INFO
                                    GROUP BY Product_Category
                                    ORDER BY Total_Revenue desc
                                    """)
            rev_by_cat_df.show()
            logger.info(
                "Total Revenue by product category\n {}".format(rev_by_cat_df.toPandas()))

        except Exception as e:
            logger.error("Analyzing Hit data Failed Due To - {}".format(e))

        finally:
            logger.info("Closing Application")


if __name__ == "__main__":
    input_file = str(sys.argv[1])
    stats = Stats()
    stats.launch(input_file)
