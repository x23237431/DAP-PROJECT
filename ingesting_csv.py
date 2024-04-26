import csv
import pymongo
from datetime import datetime
import logging
from dagster import Out, op 


@op(out=Out(bool))

def ingest_csv_to_mongo() -> bool:
    mongo_connection_string = "mongodb://dapsem1:dap_sem1@localhost:27017/admin"
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["codeenforcement"]

    violations_path = r"Code_Enforcement_All_Violations.csv"
    collection_name = "violations"  # Specify the MongoDB collection name

    logging.basicConfig(level=logging.INFO)  # Set up basic logging configuration

    try:
        with open(violations_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            all_data = []

            for row in reader:
                # Handle the ViolationDate with two possible formats
                date_str = row.get('ViolationDate', '')
                try:
                    # First, try parsing the date with the 24-hour format
                    parsed_date = datetime.strptime(date_str, '%m/%d/%Y %H:%M:%S')
                except ValueError:
                    # If it fails, try the 12-hour format with AM/PM
                    parsed_date = datetime.strptime(date_str, '%m/%d/%Y %I:%M:%S %p')

                # Convert the date to the desired format and update the row
                row['ViolationDate'] = parsed_date.strftime('%m/%d/%Y %I:%M:%S %p')

                # Convert CaseID to integer if present
                if 'CaseID' in row:
                    row['CaseID'] = int(row['CaseID'])

                all_data.append(row)

            # Insert all data into the specified MongoDB collection
            if all_data:
                db[collection_name].insert_many(all_data)

        logging.info("CSV data successfully loaded and inserted into MongoDB.")
        result = True

    except Exception as e:
        logging.error("An error occurred: %s", e)
        result = False

    return result