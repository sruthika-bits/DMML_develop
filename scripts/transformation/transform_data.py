import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging
import os
import sys
from datetime import datetime

# Get project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Set up logging
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'transformation.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_database_schema():
    """Create database schema for transformed data"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123'
        )
        
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS churn_analysis")
        cursor.execute("USE churn_analysis")
        
        # Create table for transformed features
        create_table_query = """
        CREATE TABLE IF NOT EXISTS customer_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id VARCHAR(255),
            tenure_months FLOAT,
            total_spent FLOAT,
            transaction_count INT,
            avg_transaction_value FLOAT,
            days_since_last_purchase INT,
            churn_probability FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Database schema created successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error creating database schema: {str(e)}")
        return False

def transform_data():
    """Transform data and create features"""
    try:
        # Load processed data
        processed_dir = os.path.join(project_root, 'data', 'processed')
        customer_file = os.path.join(processed_dir, 'cleaned_customers_data.csv')
        transaction_file = os.path.join(processed_dir, 'cleaned_transactions_data.csv')
        
        if not os.path.exists(customer_file):
            logging.error("Processed customer data not found")
            return None
        
        customers_df = pd.read_csv(customer_file)
        
        # Basic feature engineering
        features_df = customers_df.copy()
        
        # Create tenure features from join_date
        if 'join_date' in features_df.columns:
            features_df['join_date'] = pd.to_datetime(features_df['join_date'], errors='coerce')
            features_df['tenure_days'] = (datetime.now() - features_df['join_date']).dt.days
            features_df['tenure_months'] = features_df['tenure_days'] / 30
            features_df['tenure_years'] = features_df['tenure_days'] / 365
        else:
            logging.warning("join_date column missing; skipping tenure feature creation")
            features_df['tenure_months'] = None
            features_df['tenure_years'] = None
        
        # Load transactions if available
        if os.path.exists(transaction_file):
            transactions_df = pd.read_csv(transaction_file)
            
            # Check for required columns
            required_columns = ['customer_id', 'amount', 'transaction_date']
            if all(col in transactions_df.columns for col in required_columns):
                # Aggregate transaction data
                transaction_features = transactions_df.groupby('customer_id').agg({
                    'amount': ['sum', 'count', 'mean'],
                    'transaction_date': 'max'
                }).reset_index()
                
                transaction_features.columns = ['customer_id', 'total_spent', 'transaction_count', 
                                              'avg_transaction_value', 'last_transaction_date']
                
                # Merge with customer data
                features_df = features_df.merge(transaction_features, on='customer_id', how='left')
                
                # Calculate days since last purchase
                if 'last_transaction_date' in features_df.columns:
                    features_df['last_transaction_date'] = pd.to_datetime(features_df['last_transaction_date'], errors='coerce')
                    features_df['days_since_last_purchase'] = (datetime.now() - features_df['last_transaction_date']).dt.days
            else:
                logging.warning(f"Transaction file missing required columns: {required_columns}. Skipping transaction features.")
                features_df['total_spent'] = None
                features_df['transaction_count'] = None
                features_df['avg_transaction_value'] = None
                features_df['days_since_last_purchase'] = None
        else:
            logging.warning("Transaction file not found; skipping transaction features")
            features_df['total_spent'] = None
            features_df['transaction_count'] = None
            features_df['avg_transaction_value'] = None
            features_df['days_since_last_purchase'] = None
        
        # Select final features
        feature_columns = ['customer_id', 'tenure_months', 'total_spent', 
                          'transaction_count', 'avg_transaction_value', 'days_since_last_purchase']
        
        if 'churn' in features_df.columns:
            feature_columns.append('churn')
            
        # Keep only available columns
        available_columns = [col for col in feature_columns if col in features_df.columns]
        final_features = features_df[available_columns]
        
        logging.info(f"Transformed data shape: {final_features.shape}")
        return final_features
        
    except Exception as e:
        logging.error(f"Error transforming data: {str(e)}")
        return None

def store_transformed_data(features_df):
    """Store transformed data in database"""
    try:
        engine = create_engine('mysql+mysqlconnector://root:root123@localhost/churn_analysis')
        features_df.to_sql('customer_features', engine, if_exists='replace', index=False)
        
        logging.info(f"Transformed data stored successfully. {len(features_df)} records inserted.")
        return True
        
    except Exception as e:
        logging.error(f"Error storing transformed data: {str(e)}")
        return False

def generate_transformation_summary(features_df):
    """Generate transformation summary report"""
    try:
        reports_dir = os.path.join(project_root, 'data', 'transformation_reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = os.path.join(reports_dir, f'transformation_summary_{timestamp}.csv')
        
        # Create summary
        summary_data = []
        for column in features_df.columns:
            summary_data.append({
                'feature_name': column,
                'data_type': str(features_df[column].dtype),
                'null_count': features_df[column].isnull().sum(),
                'unique_values': features_df[column].nunique(),
                'description': 'Generated feature' if column not in ['customer_id', 'churn'] else 'Original feature'
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(summary_path, index=False)
        
        logging.info(f"Transformation summary saved: {summary_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error generating transformation summary: {str(e)}")
        return False

if __name__ == "__main__":
    logging.info("Starting data transformation process")
    print("Starting data transformation process...")
    
    # Create database schema
    schema_created = create_database_schema()
    
    if schema_created:
        # Transform data
        features_df = transform_data()
        
        if features_df is not None:
            # Store transformed data
            store_success = store_transformed_data(features_df)
            
            # Generate summary
            summary_success = generate_transformation_summary(features_df)
            
            if store_success and summary_success:
                logging.info("Data transformation completed successfully")
                print("✓ Data transformation completed successfully")
            else:
                logging.error("Data transformation completed with errors")
                print("✗ Data transformation completed with errors")
        else:
            logging.error("Data transformation failed - no features generated")
            print("✗ Data transformation failed")
    else:
        logging.error("Failed to create database schema")
        print("✗ Failed to create database schema")