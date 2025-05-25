import sys
from lib.logger import Log4j
from lib import dataManipulation, dataReader, utility
from pyspark.sql.functions import *
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)


    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = utility.get_spark_session(job_run_env)
    logger=Log4j(spark)
    logger.info("created spark session")

    print("Created Spark Session")
    orders_df = dataReader.read_orders(spark,job_run_env)
    orders_filtered = dataManipulation.filter_closed_orders(orders_df)
    customers_df = dataReader.read_customers(spark,job_run_env)
    joined_df = dataManipulation.join_orders_customers(orders_filtered,customers_df)
    aggregated_results = dataManipulation.count_orders_state(joined_df)
    aggregated_results.show()
    logger.info("spark session completed")