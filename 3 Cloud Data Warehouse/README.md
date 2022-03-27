# Cloud Data Warehouse
Implementation of a workflow for creating a Cloud Data Warehouse using Amazon Redshift. A star schema is used for the database, which is mainly intended for data analytics purposes for the imaginary music streaming startup Sparkify, containing song and log datasets.

## How to run the scripts
1. Insert your AWS credentials into dwh.cfg.
2. Create a Redshift cluster by running the respective cells in the Jupyter notebook create_dwh.ipynb.
3. Insert your HOST and ARN into dwh.cfg.
4. Run in terminal: python3 create_tables.py
5. Run in terminal: python3 etl.py

## Description of all files contained in the repository
1. **create_dwh.ipynb** is a Jupyter notebook for creating and deleting a redshift cluster using the library boto3, as well as going through the complete workflow, for setting up a Cloud Data Warehouse via Amazon Redshift. 
2. **create_tables.py** drops and creates the staging as well as other tables.
3. **etl.py** extracts the events and song data from an S3 bucket into the staging tables. In the next step, it transforms the data from the staging tables and loads it into separate tables defined by a star schema.
4. **sql_queries.py** contains all sql queries, and is imported into create_tables.py and etl.py.

## Database design
The database is modeled using a star schema: The table "songplays" is the fact table right in the center of the schema, all other tables ("songs", "artists", "time" and "users") are dimension tables linked to the fact table via their primary keys.