from src import common_utilities
from src.analyzer import USVehicleAccidentsCaseStudy


class Orchestrator:
    def __init__(self,spark):
        
        self.spark = spark

        configurations_path = 'Configurations/configurations.yaml'
        self.configurations = common_utilities.read_yaml_file(configurations_path)

        ## Ensure data directory is present
        common_utilities \
            .extract_zip_and_create_directory_if_not_exist(self.configurations.get('DATA_DIRECTORY'), \
                                                        self.configurations.get('DATA_SOURCE'))

        common_utilities. \
            ensure_all_data_is_present(self.configurations.get('DATA_DIRECTORY'), \
                                       self.configurations.get('INPUT_FILES'), \
                                        self.configurations.get('DATA_SOURCE'))    

        self.case_study = USVehicleAccidentsCaseStudy(self.configurations, self.spark)
        self.run_analyzer()
        self.codes_to_ignore = self.configurations.get('CODES_TO_IGNORE')

    def run_analyzer(self):
        """
        This function runs the analysis and generates the analysis report
        """

        ## Analysis1
        print(f'Analysis 1. {self.case_study.number_of_crashes_with_more_than_two_male_deaths()}')

        ## Analysis2
        print(f'Analysis 2. {self.case_study.number_of_two_wheelers_booked_for_crashes()}')

        ## Analysis3
        print(f'Analysis 3. {self.case_study.top_5_vehicle_makes_where_driver_dies_and_airbags_not_deployed()}')

        ## Analysis4
        print(f'Analysis 4. {self.case_study.number_of_vehicles_where_driver_has_valid_license_involved_in_hit_and_run()}')

        ## Analysis5
        print(f'Analysis 5. {self.case_study.state_with_highest_accidents_not_involving_females()}')
        
        ## Analysis6
        print(f'Analysis 6. {self.case_study.top_third_to_top_fifth_vehicle_make_ids_with_highest_injury_death_count()}')

        ## Analysis7
        print(f'Analysis 7. {self.case_study.top_ethinicity_per_vehicle_body_type()}')

        ## Analysis8    
        print(f'Analysis 8. {self.case_study.top_5_zip_codes_with_alcohol_related_crashes()}')

        ## Analysis9
        print(f'Analysis 9. {self.case_study.count_of_distinct_crash_ids_where_no_damages_and_damage_level_greater_than_4_insurance_is_availed()}')
        
        ## Analysis10
        print(f'Analysis 10. {self.case_study.get_top_5_vehicle_brand()}')