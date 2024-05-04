# DAP-PROJECT
In this project, three large datasets related to the code enforcement effort of a metropolitan area are utilized: violations, cases, and inspections. They are initially loaded into and indexed within a MongoDB database. Then, the data is transferred and transformed using dagster into its destination and final structure, a PostgreSQL environment. Finally, it is visualized using plotly, which shows in a concise and easily interpreted way the relationship between the different factors considered, which potentially allows for the identification of areas in which the current enforcement effort is less effective.

The links for the 3 datasets are:
dataset 1 https://catalog.data.gov/dataset/code-enforcement-all-violations
dataset 2 https://catalog.data.gov/dataset/code-enforcement-all-inspections
dataset 3 https://catalog.data.gov/dataset/code-enforcement-all-cases-cd955

The libraries used to run the dagster ETL tool are dagster, dagster-pandas, dagit, pandas, pendulumpsycopg2, pymongo and sqlalchemy.

The data pipeline utilizing three distinct datasets in a typical Extract, Transform, Load (ETL) process. Initially, data from Dataset 1, Dataset 2, and Dataset 3 is collected, possiblycontaining information on code enforcement violations, inspections, and case management. These are integrated or fetched from a MongoDB database during the "Getting Mongo Data" phase. Subsequently, the data undergoes transformation where it is cleaned, reformatted, or merged to fit analytical or operational needs. The final stage, "Load," involves transferring the refined data to another storage system, such as a data warehouse, for further utilization in analytics or reporting tasks. 

firstly, the"data_ingestion.py" ingests the datasets to mongoDB and is executed .Then "pipeline.py" extracts the data from mongoDB and converts the daytasets into dataframes. After transforming all the datasets, the 3 dataframes are merged into one dataframe and loaded into postgreSQL database. All the op( operators) in the above files are loaded as jobs, as dagster runs all the jobs not the decorators . The code for the same is in the "etl.py" file.


