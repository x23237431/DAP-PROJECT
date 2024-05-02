from dagster import job , repository

from data_ingestion import *

from pipeline import *

@job
def my_data_pipeline():
    # Fetching datasets
    ds1 = dataset1() # Define or import this function
    ds2 = dataset2()  # Define or import this function
    ds3 = dataset3()  # Define or import this function

    # Extracting data from MongoDB
    all_violations_df, all_inspections_df, all_cases_df = getting_mongo_data(ds1, ds2, ds3)

    # Transforming data
    mixed_df = transform(all_violations_df, all_inspections_df, all_cases_df)

    # Loading data to the database
    load(mixed_df)