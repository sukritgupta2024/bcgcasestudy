from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
import common_utilities
import yaml
import os

class USVehicleAccidentsCaseStudy:
    
    def __init__(self, configurations):
        ## Read all input files
        input_file_paths = configurations.get('INPUT_FILES')
        self.df_charges = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Charges"))
        self.df_damages = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Damages"))
        self.df_endorse = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Endorse"))
        self.df_primary_person = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Primary_Person"))
        self.df_units = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Units"))
        self.df_restrict = common_utilities.csv_to_dataframe(spark, configurations.get("DATA_DIRECTORY")+"/"+input_file_paths.get("Restrict"))

if __name__ == '__main__':

    configurations_file = 'src/configurations.yaml'
    
    ## Initialize SparkSession Object
    spark = SparkSession \
            .builder \
            .appName('USVehicleAccidentCaseStudy') \
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    configurations = common_utilities.read_yaml_file(configurations_file)

    ## Ensure data directory is present
    common_utilities.extract_zip_and_create_directory_if_not_exist(configurations.get('DATA_DIRECTORY'), configurations.get('DATA_SOURCE'))
    
    ## Ensure no files are deleted from data directory
    common_utilities.ensure_all_data_is_present(configurations.get('DATA_DIRECTORY'), configurations.get('INPUT_FILES'),configurations.get('DATA_SOURCE'))    

    case_study = USVehicleAccidentsCaseStudy(configurations)

    ## Stop Spark Session
    spark.stop()