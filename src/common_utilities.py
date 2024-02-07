import os
import zipfile
import yaml

def extract_zip_and_create_directory_if_not_exist(directory, zip_file):
    """
    Function to check if data directory exists, if not it extracts zip file data
    :param directory: path to directory
    :param zip_file: path to zip file
    """

    if(not os.path.exists(directory)):
        unzip_file(directory,zip_file)

def unzip_file(extract_to, zip_file):
    """
    Function to extract zip file to data directory
    :param extract_to: path to extract zip file to
    :param zip_file:  path to zip_file
    """

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def read_yaml_file(configuration_file_path):
    """
    Function to read yaml file
    :param utilities_path: path to yaml file
    """

    with open(configuration_file_path, "r") as f:
        return yaml.safe_load(f)

def ensure_all_data_is_present(directory, input_file_paths, zip_file):
    """
    Function to ensure all data is present
    :param directory: data directory to check
    :param input_file_paths: path to all input files
    :param zip_file: path to zip file
    """

    charges_file_path = input_file_paths.get('Charges')
    damages_file_path = input_file_paths.get('Damages')
    endorse_file_path = input_file_paths.get('Endorse')
    primary_file_path = input_file_paths.get('Primary_Person')
    restrict_file_path = input_file_paths.get('Restrict')
    units_file_path = input_file_paths.get('Units')

    files_to_extract = []

    if(not os.path.exists(directory+charges_file_path)):
        files_to_extract.append(charges_file_path)

    if(not os.path.exists(directory+damages_file_path)):
        files_to_extract.append(damages_file_path)

    if(not os.path.exists(directory+endorse_file_path)):
        files_to_extract.append(endorse_file_path)

    if(not os.path.exists(directory+primary_file_path)):
        files_to_extract.append(primary_file_path)

    if(not os.path.exists(directory+restrict_file_path)):
        files_to_extract.append(restrict_file_path)

    if(not os.path.exists(directory+units_file_path)):
        files_to_extract.append(units_file_path)

    ## Replace missing files
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            ## Extract the base filename
            file_name = file_info.filename
            if(file_name in files_to_extract):
                zip_ref.extract(file_info, directory)

def csv_to_dataframe(spark, file_path):
    """
    Read csv data into dataframe
    :param spark: SparkSession object
    :param file_path: path to file
    """

    return spark.read.option("inferSchema", "true").csv(file_path, header=True)