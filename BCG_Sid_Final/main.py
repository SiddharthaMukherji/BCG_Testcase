from pyspark.sql import SparkSession
from root.vehicle_accident_us_analyzer import VehicleAccidentUSAnalyzer
import config

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("VehicleAccidentUSAnalyzer").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    output_file_paths = config.OUTPUT_PATH
    file_format = config.FILE_FORMAT

    VA = VehicleAccidentUSAnalyzer(spark)

    print("\n")
    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "1: Number of accidents in which males killed are more than 2 are- ",
        VA.count_fatal_accidents_involving_males(output_file_paths.get(1), file_format.get("Output")),
    )

    print("\n")
    # 2. How many two-wheelers are booked for crashes?
    print(
        "2: The count of two-wheelers booked for crashes are- ",
        VA.count_accidents_involving_two_wheelers(
            output_file_paths.get(2), file_format.get("Output")
        ),
    )

    print("\n")
    # 3. Determine the Top 5 Vehicles made of the cars present in the crashes in which a driver died and Airbags did
    # not deploy.
    print(
        "3: Top 5 Vehicle companies of cars involved in crashes, resulting in death and Airbags didn't deploy- ",
        VA.top_5_vehicle_makes_in_deadly_crashes_without_airbags(
            output_file_paths.get(3), file_format.get("Output")
        ),
    )

    print("\n")
    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
    print(
        "4: Count of vehicles involved in hit & run with driver having valid licence are- ",
        VA.count_hit_and_run_with_valid_licenses(
            output_file_paths.get(4), file_format.get("Output")
        ),
    )

    print("\n")
    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "5: State having the highest number of crashes in which females are not involved is- ",
        VA.get_state_with_crashes_not_involving_females(
            output_file_paths.get(5), file_format.get("Output")
        ),
    )

    print("\n")
    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death
    print(
        "6: Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death are- ",
        VA.get_top_brands_contributing_to_injuries(
            output_file_paths.get(6), file_format.get("Output")
        ),
    )

    print("\n")
    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("7: Top ethnic user group of each unique body style involved in crashes- ")
    VA.get_top_ethnicity_per_body_style(
        output_file_paths.get(7), file_format.get("Output")
    ).show(truncate=False)

    print("\n")
    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "8: Top 5 Zip Codes with the highest number of crashes with alcohol as the contributing factor- ",
        VA.get_top_zip_codes_by_alcohol_related_crashes(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    print("\n")
    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "9: Count of Distinct Crash IDs with No Damaged Property, Damage level above 4 and car avails Insurance- ",
        VA.get_crash_ids_with_no_property_damage(
            output_file_paths.get(9), file_format.get("Output")
        ),
    )

    print("\n")
    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "10: Top 5 Vehicle Makes where drivers are charged with speeding related offences, have licensed Drivers,"
        "used top 10 used vehicle colours & have car licensed with the Top 25 states with highest number of offences- ",
        VA.get_top_5_vehicle_brand(
            output_file_paths.get(10), file_format.get("Output")
        ),
    )

    spark.stop()
