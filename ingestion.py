import pandas as pd
import os
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode='a'
)

# -------------------------------
# Function: Ingest Data in Chunks
# -------------------------------
def ingest_db(file_path, table_name, engine, chunksize=50000):
    """This function loads CSV in chunks and inserts into DB"""

    try:
        chunk_iter = pd.read_csv(file_path, chunksize=chunksize, dtype='object')

        for i, chunk in enumerate(chunk_iter):
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False
            )
            print(f"{table_name} → Chunk {i+1} loaded")

    except Exception as e:
        logging.error(f"Error loading {table_name}: {e}")


# -------------------------------
# Function: Load Raw Data
# -------------------------------
def load_raw_data(engine):
    """This function reads all CSVs and ingests into DB"""

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            ingest_db(file_path, file[:-4], engine)
            logging.info(f'Ingested {file} in DB')

    logging.info("--------- Ingestion Complete ---------")


# -------------------------------
# Main Execution
# -------------------------------


db_user = input("Enter DB username: ")
db_password = input("Enter DB password: ")
db_name = input("Enter DB name: ")
db_host = input("Enter DB host (default: localhost): ") or "localhost"

engine = create_engine(
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
)

load_raw_data(engine)