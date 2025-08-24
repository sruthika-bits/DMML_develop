import pandas as pd
import mysql.connector
from datetime import datetime
import logging
import os
import sys

# Get project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Set up logging
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'feature_store.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_database_exists():
    """Check if churn_analysis database exists"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123'
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES LIKE 'churn_analysis'")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        logging.error(f"Error checking database existence: {str(e)}")
        return False

class FeatureStore:
    def __init__(self):
        try:
            if not check_database_exists():
                logging.error("Database 'churn_analysis' does not exist")
                raise Exception("Database 'churn_analysis' does not exist")
            
            self.conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='root123',
                database='churn_analysis'
            )
            logging.info("Feature store connection established")
        except Exception as e:
            logging.error(f"Error connecting to feature store: {str(e)}")
            raise
    
    def get_features(self, customer_ids=None):
        """Retrieve features from the feature store"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            
            # Check if customer_features table exists
            cursor.execute("SHOW TABLES LIKE 'customer_features'")
            if not cursor.fetchone():
                logging.warning("Table 'customer_features' does not exist")
                cursor.close()
                return pd.DataFrame()
            
            if customer_ids:
                if isinstance(customer_ids, str):
                    customer_ids = [customer_ids]
                
                placeholders = ','.join(['%s'] * len(customer_ids))
                query = f"SELECT * FROM customer_features WHERE customer_id IN ({placeholders})"
                cursor.execute(query, tuple(customer_ids))
            else:
                query = "SELECT * FROM customer_features"
                cursor.execute(query)
                
            results = cursor.fetchall()
            cursor.close()
            
            logging.info(f"Retrieved {len(results)} records from feature store")
            return pd.DataFrame(results)
            
        except Exception as e:
            logging.error(f"Error retrieving features: {str(e)}")
            return pd.DataFrame()
    
    def add_feature_metadata(self, feature_name, description, data_type, source):
        """Add metadata for a feature"""
        try:
            cursor = self.conn.cursor()
            
            query = """
            INSERT INTO feature_metadata (feature_name, description, data_type, source, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE description=%s, data_type=%s, source=%s, updated_at=%s
            """
            
            cursor.execute(query, (feature_name, description, data_type, source, datetime.now(), 
                                 description, data_type, source, datetime.now()))
            self.conn.commit()
            cursor.close()
            
            logging.info(f"Added/updated metadata for feature: {feature_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error adding feature metadata: {str(e)}")
            return False
    
    def get_feature_metadata(self):
        """Retrieve all feature metadata"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            
            # Check if feature_metadata table exists
            cursor.execute("SHOW TABLES LIKE 'feature_metadata'")
            if not cursor.fetchone():
                logging.warning("Table 'feature_metadata' does not exist")
                cursor.close()
                return pd.DataFrame()
                
            query = "SELECT * FROM feature_metadata ORDER BY feature_name"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            logging.info(f"Retrieved {len(results)} metadata records")
            return pd.DataFrame(results)
            
        except Exception as e:
            logging.error(f"Error retrieving feature metadata: {str(e)}")
            return pd.DataFrame()

def init_feature_store():
    """Initialize the feature store with metadata table"""
    try:
        if not check_database_exists():
            logging.error("Database 'churn_analysis' does not exist")
            return False
            
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123',
            database='churn_analysis'
        )
        
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS feature_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            feature_name VARCHAR(255) NOT NULL,
            description TEXT,
            data_type VARCHAR(50),
            source VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_feature (feature_name)
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Feature store metadata table initialized")
        return True
        
    except Exception as e:
        logging.error(f"Error initializing feature store: {str(e)}")
        return False

def populate_feature_metadata():
    """Populate feature metadata"""
    try:
        fs = FeatureStore()
        
        feature_metadata = [
            ("customer_id", "Unique customer identifier", "VARCHAR", "original data"),
            ("tenure_months", "Customer tenure in months", "FLOAT", "calculated from join_date"),
            ("total_spent", "Total amount spent by customer", "FLOAT", "transaction data aggregation"),
            ("transaction_count", "Number of transactions", "INT", "transaction data count"),
            ("avg_transaction_value", "Average value per transaction", "FLOAT", "total_spent / transaction_count"),
            ("days_since_last_purchase", "Days since last transaction", "INT", "current_date - last_transaction_date"),
            ("churn", "Customer churn status (0=no, 1=yes)", "INT", "original target variable")
        ]
        
        for name, desc, dtype, source in feature_metadata:
            fs.add_feature_metadata(name, desc, dtype, source)
        
        logging.info("Feature metadata populated successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error populating feature metadata: {str(e)}")
        return False

if __name__ == "__main__":
    logging.info("Initializing feature store")
    print("Initializing feature store...")
    
    # Initialize feature store
    init_success = init_feature_store()
    
    if init_success:
        # Populate metadata
        populate_success = populate_feature_metadata()
        
        if populate_success:
            # Test feature retrieval
            fs = FeatureStore()
            features = fs.get_features()
            metadata = fs.get_feature_metadata()
            
            print(f"✓ Feature store initialized successfully")
            print(f"✓ Retrieved {len(features)} features")
            print(f"✓ Retrieved {len(metadata)} metadata entries")
            
            # Save metadata to CSV
            reports_dir = os.path.join(project_root, 'data', 'feature_reports')
            os.makedirs(reports_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_path = os.path.join(reports_dir, f'feature_metadata_{timestamp}.csv')
            metadata.to_csv(metadata_path, index=False)
            
            print(f"✓ Feature metadata saved to: {metadata_path}")
        else:
            print("✗ Failed to populate feature metadata")
    else:
        print("✗ Failed to initialize feature store")