def read_csv_data(spark, file_path):
    """
    Read CSV data
    :param spark: spark instance
    :param file_path: path to the csv file
    :return: dataframe
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def output_to_file(df, file_path, write_format):
    """
    Write data frame to csv
    :param write_format: Write file format
    :param df: dataframe
    :param file_path: output file path
    :return: None
    """
    # df = df.coalesce(1)
    df.repartition(1).write.format(write_format).mode("overwrite").option("header", "true").save(file_path)
