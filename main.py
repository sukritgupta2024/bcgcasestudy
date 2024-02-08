from pyspark.sql import SparkSession
from src.orchestrator import Orchestrator
import logging


if __name__ == '__main__':
        
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('pyspark')

    spark = SparkSession \
            .builder \
            .appName('USVehicleAccidentCaseStudy') \
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    Orchestrator(spark, logger)
