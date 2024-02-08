from pyspark.sql import SparkSession
from src.orchestrator import Orchestrator



if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .appName('USVehicleAccidentCaseStudy') \
            .getOrCreate()
    
    Orchestrator(spark)
    spark.stop()