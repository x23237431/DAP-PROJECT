import csv
import pymongo
import logging
from dagster import op 
from pymongo import MongoClient
import pandas as pd
import json


client = MongoClient('mongodb://dapsem1:dap_sem1@localhost:27017/admin')
db = client['enforcement']


@op
def dataset2() -> bool:
    file_path = "inspections.json"
    collection = db['all_inspections']

    try:
        with open(file_path, 'r') as file:
            full_data = json.load(file)
        logging.info("File loaded successfully.")

        data_entries = full_data['data']
    except FileNotFoundError:
        logging.error("File not found.")
        return False
    except json.JSONDecodeError:
        logging.error("Error decoding JSON.")
        return False
    except KeyError:
        logging.error("Key error in accessing data.")
        return False

    columns = ['row_id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'CaseID', 'CaseNo', 'location',
               'InspectionDate', 'InspectionType', 'InspectionResult', 'Inspector', 'Check']
    try:
        data_dicts = [dict(zip(columns, entry)) for entry in data_entries]
    except IndexError:
        logging.error("Index error in processing data entries.")
        return False

    try:
        collection.insert_many(data_dicts)
        logging.info(f"Inserted {len(data_dicts)} records into MongoDB.")
    except Exception as e:
        logging.error(f"Exception during MongoDB insert: {str(e)}")
        return False

    logging.info("Data ingestion completed successfully.")
    return True  # Return True if all steps completed successfully


@op
def dataset3() -> bool:
    file_path = "cases.json"
    collection = db['all_cases']

    try:
        with open(file_path, 'r') as file:
            full_data = json.load(file)
        logging.info("File loaded successfully.")

        data_entries = full_data['data']
    except FileNotFoundError:
        logging.error("File not found.")
        return False
    except json.JSONDecodeError:
        logging.error("Error decoding JSON.")
        return False
    except KeyError:
        logging.error("Key error in accessing data.")
        return False

    columns = ['row_id', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 
               'OBJECTID', 'CaseNo', 'GeoPIN', 'NextHearingDate', 'Open/Closed', 'Permit Status', 
               'Previous Hearing Result', 'Stage', 'Status Date', 'Zipcode']
    try:
        data_dicts = [dict(zip(columns, entry)) for entry in data_entries]
    except IndexError:
        logging.error("Index error in processing data entries.")
        return False

    try:
        collection.insert_many(data_dicts)
        logging.info(f"Inserted {len(data_dicts)} records into MongoDB.")
    except Exception as e:
        logging.error(f"Exception during MongoDB insert: {str(e)}")
        return False

    logging.info("Data ingestion completed successfully.")
    return True  # Return True if all steps completed successfully