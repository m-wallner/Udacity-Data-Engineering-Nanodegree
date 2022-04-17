# Data Lake
Implementation of a workflow for creating a data lake using Spark on an Amazon AWS EMR Cluster. A star schema is used for the database, which is mainly intended for data analytics purposes for the imaginary music streaming startup Sparkify, containing song and log datasets.

## How to run the scripts
1. Insert your AWS credentials into dl.cfg.
2. Create a Redshift cluster by running the respective cells in the Jupyter notebook create_dwh.ipynb.
3. Insert your HOST and ARN into dwh.cfg.
4. Run in terminal: python3 create_tables.py
5. Run in terminal: python3 etl.py

## Description of all files contained in the repository
1. **etl.py** extracts the events and song data from an S3 bucket into the staging tables. In the next step, it transforms the data from the staging tables and loads it into separate tables defined by a star schema.
2. **dl.cfg** contains the AWS login credentials.

## Database design
The database is modeled using a star schema: The table "songplays" is the fact table right in the center of the schema, all other tables ("songs", "artists", "time" and "users") are dimension tables linked to the fact table via their primary keys.