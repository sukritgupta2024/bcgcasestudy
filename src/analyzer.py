import pyspark.sql.functions as F 
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from src import common_utilities
import yaml
import os

class USVehicleAccidentsCaseStudy:
    
    def __init__(self, configurations, spark):
        
        ## Initialize spark object
        self.spark = spark

        ## Read all input files
        input_file_paths = configurations.get('INPUT_FILES')
        self.df_charges = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Charges'))
        self.df_damages = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Damages'))
        self.df_endorse = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Endorse'))
        self.df_primary_person = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Primary_Person'))
        self.df_units = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Units'))
        self.df_restrict = common_utilities.csv_to_dataframe(self.spark, configurations.get('DATA_DIRECTORY')+'/'+input_file_paths.get('Restrict'))
        self.output_file_paths = configurations.get('OUTPUT_FILES')
        self.output_format = configurations.get('OUTPUT_FORMAT')
        self.codes_to_ignore = configurations.get('CODES_TO_IGNORE')

    def number_of_crashes_with_more_than_two_male_deaths(self):
        """
        Identifies the number of crashes in which the number of male deaths is over 2
        :param self: class object
        """

        result = self.df_primary_person. \
            filter(

                (self.df_primary_person.PRSN_GNDR_ID == 'MALE') \
                & (self.df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED')

                ). \
                groupBy(self.df_primary_person.CRASH_ID).count().where(col('count')>2)
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis1'), self.output_format)
        return result.count()

    def number_of_two_wheelers_booked_for_crashes(self):
        """
        Identifies the number of crashes where 2 wheelers are involved
        :param self: class object
        """

        result = self.df_units. \
            filter(

                col('VEH_BODY_STYL_ID'). \
                contains('MOTORCYCLE')
                
                )
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis2'), self.output_format)
        return result.count()

    def top_5_vehicle_makes_where_driver_dies_and_airbags_not_deployed(self):
        """
        Identifies the top 5 vehicle makes where the airbags did not deploy and the driver dies
        :param self: class object
        """

        result = self.df_primary_person \
                    .filter(
                        (self.df_primary_person.PRSN_AIRBAG_ID == 'NOT DEPLOYED') &
                        (self.df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED') &
                        (self.df_primary_person.PRSN_TYPE_ID == 'DRIVER')
                    ) \
                    .join(
                        self.df_units.filter(~col('VEH_MAKE_ID').isin(self.codes_to_ignore)),
                        on=['CRASH_ID'],
                        how='INNER'
                    ) \
                    .groupBy('VEH_MAKE_ID') \
                    .count() \
                    .sort(col('count'), ascending=False) \
                    .limit(5)
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis3'), self.output_format)
        return [row.VEH_MAKE_ID for row in result.select('VEH_MAKE_ID').collect()]
    
    def number_of_vehicles_where_driver_has_valid_license_involved_in_hit_and_run(self):
        """
        Identifies the number of vehicles where driver has valid license and is involved in hit and run
        :param self: class object
        """

        result = self.df_units.select("CRASH_ID") \
                        .join(
                            self.df_charges.filter(self.df_charges.CHARGE.contains('HIT AND RUN')),
                            on=['CRASH_ID'],
                            how='INNER'
                        ) \
                        .join(
                            self.df_primary_person.drop('PRSN_NBR','UNIT_NBR') \
                                .filter(
                                    (self.df_primary_person.PRSN_TYPE_ID == 'DRIVER') &
                                    (self.df_primary_person.DRVR_LIC_CLS_ID != 'UNLICENSED')
                                ),
                            on=['CRASH_ID'],
                            how='INNER'
                        )
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis4'), self.output_format)
        return result.count()

    def state_with_highest_accidents_not_involving_females(self):
        """
        Identifies the state with highest number of accidents not involving females
        :param self: class object
        """

        result = self.df_primary_person \
                .filter(self.df_primary_person.PRSN_GNDR_ID != 'FEMALE') \
                .groupBy(self.df_primary_person.DRVR_LIC_STATE_ID) \
                .count() \
                .sort(col('count'), ascending=False) \
                .limit(1)

        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis5'), self.output_format)
        return [result.collect()[0]['DRVR_LIC_STATE_ID'], result.collect()[0]['count']] 

    def top_third_to_top_fifth_vehicle_make_ids_with_highest_injury_death_count(self):
        """
        Identifies the vehicle make ids with the highest casualties from top 3rd to top 5th
        :param self: class object
        """

        # store = self.df_units.withColumn('TOT_INJRY_DEATH_CNT', col('TOT_INJRY_CNT')+col('DEATH_CNT')).groupBy('VEH_MAKE_ID').sum('TOT_INJRY_DEATH_CNT').withColumnRenamed('sum(TOT_INJRY_DEATH_CNT)', 'TOT_INJRY_DEATH_CNT_AGG').sort(col('TOT_INJRY_DEATH_CNT_AGG'), ascending=False)
        store = self.df_units \
                .withColumn('TOT_INJRY_DEATH_CNT', col('TOT_INJRY_CNT')+col('DEATH_CNT')) \
                .groupBy('VEH_MAKE_ID') \
                .sum('TOT_INJRY_DEATH_CNT') \
                .withColumnRenamed('sum(TOT_INJRY_DEATH_CNT)', 'TOT_INJRY_DEATH_CNT_AGG') \
                .sort(col('TOT_INJRY_DEATH_CNT_AGG'), ascending=False)

        result = store.limit(5).subtract(store.limit(2))
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis6'), self.output_format)
        return [[row["VEH_MAKE_ID"], row["TOT_INJRY_DEATH_CNT_AGG"]] for row in result.collect()]
    
    def top_ethinicity_per_vehicle_body_type(self):
        """
        Identifies the top ethincity for every vehicle body type involved in a car crash
        :param self: class object
        """
        
        result = (
                self.df_units
                .join(self.df_primary_person, on=['CRASH_ID'], how='inner')
                .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
                .filter(~col('VEH_BODY_STYL_ID').isin(self.codes_to_ignore))
                .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
                .agg(F.count('*').alias('ETHNICITY_COUNT'))
                .withColumn('Rank', F.rank().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy(F.desc('ETHNICITY_COUNT'))))
                .filter(col('Rank') == 1)
            )
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis7'), self.output_format)
        
        return [[row["VEH_BODY_STYL_ID"], row["PRSN_ETHNICITY_ID"]] for row in result.collect()]


    def top_5_zip_codes_with_alcohol_related_crashes(self):
        """
        Identifies the top 5 zip codes where alcohol caused crashes
        :param self: class object
        """

        result = (
            self.df_primary_person
            .filter(~col('DRVR_ZIP').isin(self.codes_to_ignore))
            .filter(col('PRSN_ALC_RSLT_ID') == 'Positive')
            .groupBy('DRVR_ZIP')
            .agg(F.count('*').alias('ZIP_CODE_COUNT'))
            .orderBy('ZIP_CODE_COUNT', ascending=False)
            .limit(5)
        )

        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis8'), self.output_format)
        return [[row['DRVR_ZIP'], row['ZIP_CODE_COUNT']] for row in result.collect()]

    def count_of_distinct_crash_ids_where_no_damages_and_damage_level_greater_than_4_insurance_is_availed(self):
        """
        Counts the the number of distinct crash ids where no damages are observed,
        damage level is above 4 and car avails insurance
        """
        result = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner'). \
            filter(
            (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
        ). \
            filter(self.df_damages.DAMAGED_PROPERTY == "NONE"). \
            filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        
        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis9'), self.output_format)
        return result.count()

    def get_top_5_vehicle_brand(self):
        """
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        :param output_format: Write file format
        :param output_path: output file path
        :return List of Vehicle brands
        """

        top_25_state_list = [row[0] for row in self.df_units
                    .filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
                    .groupby("VEH_LIC_STATE_ID")
                    .count()
                    .orderBy(col("count").desc())
                    .limit(25)
                    .collect()]

        top_10_used_vehicle_colors = [row[0] for row in self.df_units
                    .filter(self.df_units.VEH_COLOR_ID != "NA")
                    .groupby("VEH_COLOR_ID")
                    .count()
                    .orderBy(col("count").desc())
                    .limit(10)
                    .collect()]

        result = self.df_charges.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(self.df_units, on=['CRASH_ID'], how='inner'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        common_utilities.dataframe_to_parquet_output(result, self.output_file_paths.get('Analysis10'), self.output_format)

        return [row[0] for row in result.collect()]
    
