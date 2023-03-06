import os
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes
import pandas as pd
import csv
#read csv data in a pandas df
path="" #add path to datafile
data=pd.read_csv(path)

# Set the Cloud SQL connection properties
connection_name = ""
db_user = ""
db_password = ""
db_name = ""
driver_name = ""
query_string =  dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})

# Create a Cloud SQL connector
connector = Connector()

# Connect to the Cloud SQL instance
ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC
engine = sqlalchemy.create_engine(
    f"{driver_name}://{db_user}:{db_password}@/{db_name}",
    creator=lambda: connector.connect(
        connection_name,
        "pg8000",
        user=db_user,
        password=db_password,
        db=db_name,
        ip_type=ip_type,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800
)

# Create a table
with engine.connect() as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id Integer PRIMARY KEY,
             name VARCHAR(200) NOT NULL,
            marks Float)
           ;
    """)
    
#test table
with engine.connect() as conn:
    result = conn.execute("""SELECT column_name, data_type
                             FROM information_schema.columns
                             WHERE table_name = 'students';""")
    for row in result:
        print(row)

#insert values from csv file
with engine.connect() as conn:
    query = """INSERT INTO students (id, name,marks )
               VALUES (%s, %s, %s)"""
    for row in data.itertuples(index=False):
        values = tuple(row)
        conn.execute(query, values)


#select data from table
with engine.connect() as conn:
    result = conn.execute("""SELECT *
FROM students;""")
    for row in result:
        print(row)


#delete the records
with engine.connect() as conn:
    # Delete all students with marks less than 60
    query="DELETE FROM students WHERE id='123';"
    conn.execute(query)




