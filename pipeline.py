from dagster import op, Out, Output, DynamicOut, DynamicOutput, OpExecutionContext , In, job
import pymongo
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
from data_ingestion import *
import re
import psycopg2

#op
@op(
    ins={"dataset1": In(), "dataset2": In()},
    out={
        "all_violations_df": Out(dagster_type=pd.DataFrame),
        "all_inspections_df": Out(dagster_type=pd.DataFrame),
        "all_cases_df": Out(dagster_type=pd.DataFrame)
    }
)
def getting_mongo_data(context: OpExecutionContext,  dataset1, dataset2 , dataset3 ):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://dapsem1:dap_sem1@localhost:27017/admin")
        db = client["enforcement"]

        # Retrieve and convert data from "enforcement" collection
        all_violations = list(db["all_violations"].find())
        all_inspections = list(db["all_inspections"].find())
        all_cases = list(db["all_cases"].find())
        context.log.info("Fetched and converted enforcement data.")

        # Retrieve and convert data from  collection
        all_violations_df = pd.DataFrame(all_violations)
        all_inspections_df = pd.DataFrame(all_inspections)
        all_cases_df = pd.DataFrame(all_cases)
        context.log.info("Fetched and converted occupancy data.")

        # Output the dataframes
        return Output(all_violations_df, "all_violations_df"), Output(all_inspections_df, "all_inspections_df"), Output(all_cases_df, "all_cases_df")

    except pymongo.errors.ConnectionError as e:
        context.log.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise


@op(
    ins={
        "all_violations_df": In(dagster_type=pd.DataFrame),
        "all_inspections_df": In(dagster_type=pd.DataFrame),
        "all_cases_df": In(dagster_type=pd.DataFrame)
    },
    out=Out(dagster_type=pd.DataFrame)
)
def transform(context: OpExecutionContext, all_violations_df: pd.DataFrame, all_inspections_df: pd.DataFrame, all_cases_df: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Starting the data transformation process.")
    
    try:
        # Drop unnecessary columns
        all_violations_df.drop(columns=['_id', 'LastUpload'], inplace=True)
        all_inspections_df.drop(columns=['_id', 'row_id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7'], inplace=True)
        all_cases_df.drop(columns=['_id', 'row_id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col8', 'col7', 'NextHearingDate'], inplace=True)
        context.log.debug("Dropped unnecessary columns.")

        # Convert columns to integer

        int_violations = ['CaseID','ViolationID']

        int_inspections = ['CaseID','Check']

        int_cases =  ['OBJECTID','Zipcode']

        for col in int_violations:
            all_violations_df[col] = all_violations_df[col].fillna(0).astype(int)

        for col in int_inspections:
            all_inspections_df[col] = all_inspections_df[col].fillna(0).astype(int)
            
        for col in int_cases:
            all_cases_df[col] = all_cases_df[col].fillna(0).astype(int) 

        #using regex created a new column name Place in all the datasets
        all_violations_df['place'] = all_violations_df['Location'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
        all_inspections_df['place'] = all_inspections_df['location'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
        all_cases_df['Status Date'] = pd.to_datetime(all_cases_df['Status Date'], format='%Y%m%d%H%M%S.%f')
        all_cases_df['Status Date'] = all_cases_df['Status Date'].dt.date
        all_inspections_df['InspectionDate'] = pd.to_datetime(all_inspections_df['InspectionDate'], errors='coerce')
        all_inspections_df['InspectionDate'] = all_inspections_df['InspectionDate'].dt.date
        all_violations_df['ViolationDate'] = pd.to_datetime(all_violations_df['ViolationDate'], format='%m/%d/%Y %I:%M:%S %p')
        all_violations_df['ViolationDate'] = all_violations_df['ViolationDate'].dt.date


        #Defined the datatypes of each column
        all_violations_df = all_violations_df.astype({
            'CaseID': int, 
            'ViolationID' : int,
            'CaseNo' :  'string',
            'Location' : 'string',
            'ViolationDate' : 'datetime64[ns]',
            'CodeSection' : 'string',
            'Violation' : 'string',
            'Description' : 'string',
            'place' : 'string',
        })
        all_inspections_df=all_inspections_df.astype({
            'CaseID': int, 
            'CaseNo' : 'string',
            'location' : 'string',
            'InspectionDate' : 'datetime64[ns]',
            'InspectionType' : 'string',
            'InspectionResult' : 'string',
            'Inspector' : 'string',
            'place' : 'string',
            'Check' : int,
        })
        all_cases_df=all_cases_df.astype({
            'OBJECTID': int, 
            'CaseNo' : 'string',
            'GeoPIN' : 'string',
            'Status Date' : 'datetime64[ns]',
            'Open/Closed' : 'string',
            'Permit Status' : 'string',
            'Previous Hearing Result' : 'string',
        'Stage':'string',
            'Zipcode' : int,
        })
    
    
        # List to hold all dataframes
        dfs = [all_violations_df, all_cases_df, all_inspections_df]

        # List to hold the names of dataframes
        df_names = ['all_violations_df', 'all_cases_df', 'all_inspections_df']

        # Dictionary to keep track of column names and their counts
        column_counts = {}

        # Iterate over each DataFrame
        for df, df_name in zip(dfs, df_names):
            # Iterate over each column in the DataFrame
            for column in df.columns:
                # If column name exists in the dictionary, increment its count
                if column in column_counts:
                    column_counts[column] += 1
                # Otherwise, initialize its count to 1
                else:
                    column_counts[column] = 1

        # Iterate over each DataFrame again to rename columns
        for df, df_name in zip(dfs, df_names):
            # Iterate over each column in the DataFrame
            for column in df.columns:
                # If the column exists in more than one DataFrame, rename it
                if column_counts[column] > 1:
                    df.rename(columns={column: df_name + '_' + column}, inplace=True)
        all_violations_df = all_violations_df.rename(str.upper, axis='columns')
        all_cases_df = all_cases_df.rename(str.upper, axis='columns')
        all_inspections_df = all_inspections_df.rename(str.upper, axis='columns')


        context.log.debug("Extracted and cleaned location data.")

        #merged all the three datasets 
        mixed_df = pd.concat([all_violations_df, all_inspections_df, all_cases_df], ignore_index=True)
        
        context.log.info("Combined dataframes into a single DataFrame.")
        return mixed_df
        #return Output(mixed_df, "mixed_df")
    except Exception as e:
        context.log.error(f"An error occurred during transformation and load: {e}")
        raise


@op(
    ins={
        "mixed_df": In(dagster_type=pd.DataFrame)
    }
)
def load(context: OpExecutionContext, mixed_df: pd.DataFrame):
    context.log.info("Preparing to push data to the PostgreSQL database.")

    # Construct connection string
    connection_string = 'postgresql://postgres:groupg@localhost:5432/group_g'
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Push DataFrame to PostgreSQL
    mixed_df.to_sql("enforcement_table", engine, if_exists='replace', index=False)
    
    context.log.info("Data has been successfully pushed to PostgreSQL database.")
