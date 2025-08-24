import pandas as pd
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine
import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/storage.log'),
        logging.StreamHandler()
    ]
)

class RawDataStorage:
    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string)
        self.raw_storage_path = '../data/raw_storage/'
        os.makedirs(self.raw_storage_path, exist_ok=True)
    
    def store_in_database(self, file_path, table_name):
        """Store raw data in MySQL database"""
        try:
            df = pd.read_csv(file_path)
            df['ingestion_timestamp'] = datetime.now()
            
            # Store in database
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            logging.info(f'Successfully stored data in {table_name} table')
            
            # Also store as backup file
            self.store_as_backup(file_path, table_name)
            
            return True
        except Exception as e:
            logging.error(f'Error storing data in database: {str(e)}')
            return False
    
    def store_as_backup(self, file_path, table_name):
        """Store raw data as backup files"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f'{self.raw_storage_path}{table_name}/{timestamp}/'
            os.makedirs(backup_path, exist_ok=True)
            
            # Copy file to backup location
            import shutil
            shutil.copy2(file_path, backup_path)
            logging.info(f'Backup stored at: {backup_path}')
            
        except Exception as e:
            logging.error(f'Error creating backup: {str(e)}')

def main():
    # MySQL connection string
    db_connection_string = 'mysql+mysqlconnector://username:password@localhost/mydb'
    
    storage = RawDataStorage(db_connection_string)
    
    # Find latest ingested files
    raw_files = [f for f in os.listdir('../data/raw/') if f.endswith('.csv')]
    
    for file in raw_files:
        if 'customers' in file:
            storage.store_in_database(f'../data/raw/{file}', 'customers_raw')
        elif 'transactions' in file:
            storage.store_in_database(f'../data/raw/{file}', 'transactions_raw')

if __name__ == '__main__':
    main()