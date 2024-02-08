from src import common_utilities
from src.analyzer import USVehicleAccidentsCaseStudy


class Orchestrator:
    def __init__(self,spark, logger):
        
        self.spark = spark
        self.logger = logger
        configurations_path = 'Configurations/configurations.yaml'
        self.configurations = common_utilities.read_yaml_file(configurations_path)

        ## Ensure data directory is present
        common_utilities \
            .extract_zip_and_create_directory_if_not_exist(self.configurations.get('DATA_DIRECTORY'), \
                                                        self.configurations.get('DATA_SOURCE'))
        ## Ensure that all the files are present
        common_utilities. \
            ensure_all_data_is_present(self.configurations.get('DATA_DIRECTORY'), \
                                       self.configurations.get('INPUT_FILES'), \
                                        self.configurations.get('DATA_SOURCE'))    

        
        self.case_study = USVehicleAccidentsCaseStudy(self.configurations, self.spark)

        ## Call the analyzer function to call the analysis function from the case study class
        self.logger.info('Starting Analysis...')
        self.run_analyzer()
        self.codes_to_ignore = self.configurations.get('CODES_TO_IGNORE')

    def run_analyzer(self):
        """
        This function runs the analysis and generates the analysis report
        """

        analysis_result = ""
        analysis_methods = [
        self.case_study.number_of_crashes_with_more_than_two_male_deaths,
        self.case_study.number_of_two_wheelers_booked_for_crashes,
        self.case_study.top_5_vehicle_makes_where_driver_dies_and_airbags_not_deployed,
        self.case_study.number_of_vehicles_where_driver_has_valid_license_involved_in_hit_and_run,
        self.case_study.state_with_highest_accidents_not_involving_females,
        self.case_study.top_third_to_top_fifth_vehicle_make_ids_with_highest_injury_death_count,
        self.case_study.top_ethinicity_per_vehicle_body_type,
        self.case_study.top_5_zip_codes_with_alcohol_related_crashes,
        self.case_study.count_of_distinct_crash_ids_where_no_damages_and_damage_level_greater_than_4_insurance_is_availed,
        self.case_study.get_top_5_vehicle_brand
        ]

        for i, method in enumerate(analysis_methods, start=1):
            try:
                self.logger.info(f'Started Analysis number: {i}')
                analysis_result += f'Analysis {i}. {method()}\n'
            except Exception as e:
                self.logger.error(f'Something went wrong with Analysis {i}: {e.args[0]}')
            finally:
                self.logger.info(f'Completed Analysis number: {i}')
        
        self.logger.info(f'Completed Analysis...')
        print(analysis_result)