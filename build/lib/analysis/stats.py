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

class Stats:

    def launch(self, input_file_path):
        logger = Logger().get_logger(cfg.app_name)

        try:
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
            # Explode product_list
            products_df = ha.explode_products_list(initial_df)

            # Adding Total revenue column
            products_total_rev_df = ha.add_total_revenue(products_df)

            # Calculate total revenue by ip
            purchase_df = ha.get_total_rev_by_ip(products_total_rev_df)

            # Get external search engine except eshopzill
            ext_domain_df = ha.get_ext_search_engine_except_esshopzilla(initial_df)

            # Get total revenue by domain
            domain_rev_df = ha.get_domain_rev(ext_domain_df, purchase_df)

            # Get Search Query keywords and revenue by domain
            result_df = ha.get_agg_keywords_rev(domain_rev_df)

            # To write Spark Dataframe as .tsv file, first convert it into Pandas dataframe
            pandas_df = ha.convert_to_pandas(result_df)

            # Write Pandas Dataframe result to output file path
            ha.write_df(pandas_df, output_file_path)

            logger.info("Output written to file path at - {}".format(output_file_path))

        except Exception as e:
            logger.error("Analyzing Hit data Failed Due To - {}".format(e))

        finally:
            logger.info("Closing application")


if __name__ == "__main__":
    # Prepare logger to log info, warning, error
    logger = Logger().get_logger(cfg.app_name)

    try:
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
        # Explode product_list
        products_df = ha.explode_products_list(initial_df)

        # Adding Total revenue column
        products_total_rev_df = ha.add_total_revenue(products_df)

        # Calculate total revenue by ip
        purchase_df = ha.get_total_rev_by_ip(products_total_rev_df)

        # Get external search engine except eshopzill
        ext_domain_df = ha.get_ext_search_engine_except_esshopzilla(initial_df)

        # Get total revenue by domain
        domain_rev_df = ha.get_domain_rev(ext_domain_df, purchase_df)

        # Get Search Query keywords and revenue by domain
        result_df = ha.get_agg_keywords_rev(domain_rev_df)

        # To write Spark Dataframe as .tsv file, first convert it into Pandas dataframe
        pandas_df = ha.convert_to_pandas(result_df)

        # Write Pandas Dataframe result to output file path
        ha.write_df(pandas_df, output_file_path)

        logger.info("Output written to file path at - {}".format(output_file_path))

    except Exception as e:
        logger.error("Analyzing Hit data Failed Due To - {}".format(e))

    finally:
        logger.info("Closing application")
