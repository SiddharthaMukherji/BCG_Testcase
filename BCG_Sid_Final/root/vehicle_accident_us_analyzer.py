from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from root.utils import read_csv_data, output_to_file
import config


class VehicleAccidentUSAnalyzer:
    def __init__(self, spark):
        self.df_charges = read_csv_data(spark, config.INPUT_FILENAME.get("Charges"))
        self.df_damages = read_csv_data(spark, config.INPUT_FILENAME.get("Damages"))
        self.df_endorse = read_csv_data(spark, config.INPUT_FILENAME.get("Endorse"))
        self.df_primary_person = read_csv_data(spark, config.INPUT_FILENAME.get("Primary_Person"))
        self.df_restrict = read_csv_data(spark, config.INPUT_FILENAME.get("Restrict"))
        self.df_units = read_csv_data(spark, config.INPUT_FILENAME.get("Units"))

    def count_fatal_accidents_involving_males(self, output_path, output_format):
        """
            Counts the number of accidents where more than 2 males were killed.

            Args:
                output_path (str): The file path for the output file.
                output_format (str): The file format for writing the output.

            Returns:
                int: The count of accidents meeting the criteria.
        """

        # Filter for male accidents with more than 2 deaths
        df_filtered = (self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "MALE")
                       .filter(self.df_primary_person.DEATH_CNT > 2))
        # Writing the results to the output file
        output_to_file(df_filtered, output_path, output_format)

        return df_filtered.count()

    def count_accidents_involving_two_wheelers(self, output_path, output_format):
        """
        Finds how many two-wheelers are booked for crashes

        Args:
            output_path (str): The file path for the output file.
            output_format (str): The file format for writing the output.
        Returns:
        - int: The count of crashes in which two-wheelers are booked
        """
        df_filtered = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        output_to_file(df_filtered, output_path, output_format)

        return df_filtered.count()

    def top_5_vehicle_makes_in_deadly_crashes_without_airbags(self, output_path, output_format):
        """
        Finds the top 5 Vehicle Makes of the cars involved in crashes where the driver died and airbags did not
        deploy.
        Args:
            - output_format (str): The file format for writing the output.
            - output_path (str): The
        file path for the output file.
        Returns: - List[str]: Top 5 vehicles Make for killed crashes without an airbag
        deployment.

        """
        # Joining the units and primary_person dataframes
        df_joined = self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")

        # filtering on the basis of required columns
        df_filtered = df_joined.filter(
            (col("PRSN_INJRY_SEV_ID") == "KILLED") &
            (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
            (col("VEH_MAKE_ID") != "NA")
        )
        # Group by vehicle make, counting occurrences, sorting by count in desc order and limit top 5
        df_grouped = df_filtered.groupby("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)

        # Writing the results to the output file
        output_to_file(df_grouped, output_path, output_format)

        # Extract the 'VEH_MAKE_ID' from the top 5 rows and return as a list
        return [row[0] for row in df_grouped.collect()]

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
        """
        Finds number of Vehicles with driver having valid licences involved in hit and run.
        Args:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.
        """

        # joining units and primary_person dataframes selecting required columns
        df_joined = self.df_units.select("CRASH_ID", "VEH_HNR_FL").join(
            self.df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"), on=["CRASH_ID"], how="inner")

        # filtering for hit & run flag=y and valid driving license
        df_filtered = df_joined.filter((col("VEH_HNR_FL") == "Y") &
                                       (col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])))

        # Writing the results to the output file
        output_to_file(df_filtered, output_path, output_format)

        return df_filtered.count()

    def get_state_with_crashes_not_involving_females(self, output_path, output_format):
        """
        Finds the state which has the highest number of accidents in which females are not involved

        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - str: The state with the highest number of accidents without female involvement.
        """
        # Filtering for accidents not involving females
        df_no_female_accidents = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID != "FEMALE")

        # Group by driver's license state and count accidents
        df_grouped_by_state = df_no_female_accidents.groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())

        # Writing the results to the output file
        output_to_file(df_grouped_by_state, output_path, output_format)

        # Returning the state with the highest count
        return df_grouped_by_state.first().DRVR_LIC_STATE_ID

    def get_top_brands_contributing_to_injuries(self, output_path, output_format):
        """
        Finds the VEH_MAKE_IDs ranking from the 3rd to the 5th positions that contribute to the largest number of
        injuries, including death.
        Parameters: - output_format (str): The file format for writing the output. -
        output_path (str): The file path for the output file.
        Returns: - List[int]: The Top 3rd to 5th VEH_MAKE_IDs
        that contribute to the largest number of injuries, including death.
        """
        # Filtering out rows with missing vehicle make IDs
        df_filtered = self.df_units.filter(self.df_units['VEH_MAKE_ID'] != "NA")

        # Calculating total casualties by adding injury and death counts
        df_with_casualties = df_filtered.withColumn("TOTAL_CASUALTIES", col("TOT_INJRY_CNT") + col("DEATH_CNT"))

        # Grouping by vehicle make, sum total casualties, and sort in descending order
        df_grouped = (
            df_with_casualties.groupby("VEH_MAKE_ID")
            .sum("TOTAL_CASUALTIES")
            .withColumnRenamed("sum(TOTAL_CASUALTIES)", "TOTAL_CASUALTIES_AGG")
            .orderBy(col("TOTAL_CASUALTIES_AGG").desc())
        )

        # Extracting 3rd to 5th ranked vehicle makes
        df_top_3_to_5 = df_grouped.limit(5).subtract(df_grouped.limit(2))

        # Writing the results to the output file
        output_to_file(df_top_3_to_5, output_path, output_format)

        # Return the list of top vehicle makes
        return [row[0] for row in df_top_3_to_5.select("VEH_MAKE_ID").collect()]

    def get_top_ethnicity_per_body_style(self, output_path, output_format):
        """
        For all the body styles involved in crashes , finds the top ethnic user group of each unique body style
        Args:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - dataframe
        """
        # Filter out invalid or missing body styles and ethnicities
        filtered_units = self.df_units.filter(
            ~col("VEH_BODY_STYL_ID").isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])
        )
        filtered_persons = self.df_primary_person.filter(~col("PRSN_ETHNICITY_ID").isin(["NA", "UNKNOWN"]))

        # Join units and primary person data
        joined_df = filtered_units.join(filtered_persons, on=["CRASH_ID"], how="inner")

        # Count occurrences of each (body style, ethnicity) combination
        count_df = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()

        # Define window partitioning and ordering for row_number
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

        # Add row number within each window partition
        ranked_df = count_df.withColumn("row", row_number().over(window_spec))

        # Filter to keep only the top row (highest count) for each body style
        top_ethnic_df = ranked_df.filter(col("row") == 1).drop("row", "count")

        # Write output to file
        output_to_file(top_ethnic_df, output_path, output_format)

        return top_ethnic_df

    def get_top_zip_codes_by_alcohol_related_crashes(self, output_path, output_format):
        """
        Identifies the top 5 Zip Codes with the most crashes where alcohol is a contributing factor.

        Args:
            output_path (str): The file path to write the output.
            output_format (str): The format of the output file.

        Returns:
            list[str]: A list containing the top 5 Zip Codes.
        """

        # Join units and primary person DataFrames
        joined_df = self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")

        # Filter out rows with null 'DRVR_ZIP' and where alcohol is a contributing factor
        filtered_df = joined_df.dropna(subset=["DRVR_ZIP"]).filter(
            (col("CONTRIB_FACTR_1_ID").contains("ALCOHOL"))
            | (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))
        )

        # Group by 'DRVR_ZIP', count occurrences, and get the top 5
        top_zip_codes_df = (
            filtered_df.groupBy("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        # Write the results to the specified output file
        output_to_file(top_zip_codes_df, output_path, output_format)

        # Extract and return the top 5 zip codes as a list
        return [row["DRVR_ZIP"] for row in top_zip_codes_df.collect()]

    def get_crash_ids_with_no_property_damage(self, output_path, output_format):
        """
        Gets the count of Distinct Crash IDs where No Damaged Property was observed and Damage Level
        is above 4 and car avails Insurance

        Args:
            output_path (str): The file path for the output file.
            output_format (str): The file format for writing the output.

        Returns:
            list[str]: A list of distinct Crash IDs meeting the criteria.
        """

        # Filter units DataFrame based on damage levels
        filtered_units = self.df_units.filter(
            (
                    (col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4")
                    & (~col("VEH_DMAG_SCL_1_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
            | (
                    (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")
                    & (~col("VEH_DMAG_SCL_2_ID").isin(["NA", "NO DAMAGE", "INVALID VALUE"]))

            )
        )

        # Join damages and filtered units DataFrames
        joined_df = self.df_damages.join(filtered_units, on=["CRASH_ID"], how="inner")

        # Filter based on no property damage and insurance presence
        result_df = joined_df.filter(
            (col("DAMAGED_PROPERTY") == "NONE")
            & (col("FIN_RESP_TYPE_ID") == "PROOF OF LIABILITY INSURANCE")
        )

        # Write output to file
        output_to_file(result_df, output_path, output_format)

        # Collect distinct Crash IDs and return as a list
        crash_ids=[row["CRASH_ID"] for row in result_df.select("CRASH_ID").distinct().collect()]

        # Calculate count and create the dictionary

        result_dict = {"Count of crash id": len(crash_ids), "Crash Ids": crash_ids}

        return result_dict

    def get_top_5_vehicle_brand(self, output_path, output_format):
        """
        Determines the top 5 Vehicle Makes/Brands meeting the following criteria:
        - Drivers are charged with speeding-related offenses
        - Drivers have licenses (either driver's license or commercial driver's license)
        - Vehicles are among the top 10 used colors
        - Vehicles are licensed in the top 25 states with the highest number of offenses

        Args:
            output_path (str): The file path for the output file.
            output_format (str): The file format for writing the output.

        Returns:
            list[str]: The list of top 5 Vehicle Makes/Brands meeting the criteria.
        """

        # Get top 25 states with the highest number of offenses
        top_25_states_df = (
            self.df_units
            .filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
        )
        top_25_state_list = [row["VEH_LIC_STATE_ID"] for row in top_25_states_df.collect()]

        # Get top 10 used vehicle colors
        top_10_colors_df = (
            self.df_units
            .filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
        )
        top_10_used_vehicle_colors = [row["VEH_COLOR_ID"] for row in top_10_colors_df.collect()]

        # Join and filter data

        filtered_df = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
        )

        # Group by vehicle make, count, and get top 5
        top_5_brands_df = (
            filtered_df
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        # Write output to file
        output_to_file(top_5_brands_df, output_path, output_format)

        # Return top 5 vehicle brands
        return [row["VEH_MAKE_ID"] for row in top_5_brands_df.collect()]
