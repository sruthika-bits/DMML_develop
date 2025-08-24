# scripts/ingestion/ingest_data.py
import pandas as pd
import requests
import mysql.connector
from datetime import datetime
import logging
import os

# Set up logging
logging.basicConfig(
    filename='../dmml_asisgnment/logs/ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ingest_from_sql():
    """Ingest data from SQL database"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123',
            database='mydb'
        )
        
        query = "SELECT * FROM customers"
        df = pd.read_sql(query, conn)
        
        # Save raw data with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"../dmml_asisgnment/data/raw/customers_{timestamp}.csv"
        df.to_csv(filename, index=False)
        
        logging.info(f"Successfully ingested {len(df)} records from SQL database")
        conn.close()
        return True
        
    except Exception as e:
        logging.error(f"Error ingesting from SQL: {str(e)}")
        return False

def ingest_from_api():
    """Ingest data from mock transaction API"""
    try:
        # Mock API endpoint - in real scenario, replace with actual API
        api_url = "https://jsonplaceholder.typicode.com/users"
        response = requests.get(api_url)
        
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            
            # Save raw data with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"../dmml_asisgnment/data/raw/transactions_{timestamp}.csv"
            df.to_csv(filename, index=False)
            
            logging.info(f"Successfully ingested {len(df)} records from API")
            return True
        else:
            logging.error(f"API request failed with status code: {response.status_code}")
            return False
            
    except Exception as e:
        logging.error(f"Error ingesting from API: {str(e)}")
        return False

if __name__ == "__main__":
    logging.info("Starting data ingestion process")
    
    # Create data directory if it doesn't exist
    os.makedirs('../data/raw', exist_ok=True)
    
    # Ingest from both sources
    sql_success = ingest_from_sql()
    api_success = ingest_from_api()
    
    if sql_success and api_success:
        logging.info("Data ingestion completed successfully")
    else:
        logging.warning("Data ingestion completed with errors")