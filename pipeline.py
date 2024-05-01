from dagster import op, Out, Output, DynamicOut, DynamicOutput, OpExecutionContext , In, job
import pymongo
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
from data_ingestion import *
import re
import psycopg2

@op(
    ins={"dataset3": In()},
    out={
        "all_cases_df": Out(dagster_type=pd.DataFrame)
    }
)
def getting_mongo_data(context: OpExecutionContext, dataset3 ):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://dapsem1:dap_sem1@localhost:27017/admin")
        db = client["enforcement"]

        # Retrieve and convert data from "enforcement" collection
        all_cases = list(db["all_cases"].find())
        context.log.info("Fetched and converted enforcement data.")

        # Retrieve and convert data from "occupancy" collection
        all_cases_df = pd.DataFrame(all_cases)
        context.log.info("Fetched and converted occupancy data.")

        # Output the dataframes
        return Output(all_cases_df, "all_cases_df")

    except pymongo.errors.ConnectionError as e:
        context.log.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise


@op(
    ins={
        "all_cases_df": In(dagster_type=pd.DataFrame)
    },
    out=Out(dagster_type=pd.DataFrame)
)
def transform(context: OpExecutionContext, all_cases_df: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Starting the data transformation process.")
    
    try:
        # Drop unnecessary columns
        all_cases_df.drop(columns=['_id', 'row_id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col8', 'col7', 'NextHearingDate'], inplace=True)
        context.log.debug("Dropped unnecessary columns.")

        # Convert columns to integer
        int_columns = {
            'all_cases_df': ['OBJECTID', 'Zipcode']
        }
        for df_name, cols in int_columns.items():
            for col in cols:
                locals()[df_name][col] = locals()[df_name][col].fillna(0).astype(int)
        context.log.info("Converted specified columns to integer.")

        # Convert datetime columns
        all_cases_df['STATUS DATE'] = pd.to_datetime(all_cases_df['Status Date'], format='%Y%m%d%H%M%S.%f').dt.date
        context.log.info("Converted datetime columns.")

        # Rename overlapping columns before merging
        dfs = [all_cases_df]
        df_names = ['all_cases_df']
        column_counts = {}
        for df, df_name in zip(dfs, df_names):
            for column in df.columns:
                if column in column_counts:
                    column_counts[column] += 1
                else:
                    column_counts[column] = 1
        for df, df_name in zip(dfs, df_names):
            for column in df.columns:
                if column_counts[column] > 1:
                    df.rename(columns={column: df_name + '_' + column}, inplace=True)
        


