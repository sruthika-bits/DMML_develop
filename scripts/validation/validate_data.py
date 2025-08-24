# scripts/validation/validate_data.py
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import sys

# Get the project root directory
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'validation.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def validate_data(file_path):
    """Validate data and generate quality report"""
    try:
        df = pd.read_csv(file_path)
        filename = os.path.basename(file_path)
        
        # Initialize validation results
        validation_results = []
        
        # Check for each column
        for column in df.columns:
            # Check for missing values
            null_count = df[column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            
            # Check data type consistency
            dtype = str(df[column].dtype)
            
            # Check for unique values
            unique_count = df[column].nunique()
            
            # Check for potential duplicates (for key columns)
            has_duplicates = False
            if column in ['id', 'customer_id', 'transaction_id']:
                has_duplicates = df[column].duplicated().any()
            
            # Validate specific data types
            data_issues = []
            if 'email' in column.lower() and dtype == 'object':
                invalid_emails = df[df[column].notnull() & ~df[column].str.contains('@', na=False)]
                if len(invalid_emails) > 0:
                    data_issues.append(f"{len(invalid_emails)} invalid email formats")
            
            if column in ['amount', 'price', 'value'] and dtype in ['int64', 'float64']:
                negative_values = df[df[column] < 0]
                if len(negative_values) > 0:
                    data_issues.append(f"{len(negative_values)} negative values")
            
            # Add to validation results
            validation_results.append({
                'file_name': filename,
                'column_name': column,
                'data_type': dtype,
                'total_values': len(df),
                'null_count': null_count,
                'null_percentage': f"{null_percentage:.2f}%",
                'unique_values': unique_count,
                'has_duplicates': has_duplicates,
                'data_issues': '; '.join(data_issues) if data_issues else 'None',
                'validation_status': 'PASS' if null_count == 0 and not has_duplicates and not data_issues else 'FAIL'
            })
        
        return validation_results, df
        
    except Exception as e:
        logging.error(f"Error validating {file_path}: {str(e)}")
        return [], None

def generate_quality_report(validation_results, output_path):
    """Generate CSV quality report"""
    if validation_results:
        report_df = pd.DataFrame(validation_results)
        report_df.to_csv(output_path, index=False)
        return True
    return False

if __name__ == "__main__":
    print("Starting data validation process...")
    logging.info("Starting data validation process")
    
    # Create directories if they don't exist
    validated_dir = os.path.join(project_root, 'data', 'validated')
    raw_dir = os.path.join(project_root, 'data', 'raw')
    reports_dir = os.path.join(project_root, 'data', 'validation_reports')
    
    os.makedirs(validated_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    
    # Get all CSV files in raw directory
    raw_files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
    
    if not raw_files:
        logging.warning("No CSV files found in raw data directory")
        print("No CSV files found in raw data directory. Please run ingestion first.")
    else:
        print(f"Found {len(raw_files)} CSV file(s) to validate:")
        for file in raw_files:
            print(f"  - {file}")
        
        all_validation_results = []
        
        for file in raw_files:
            file_path = os.path.join(raw_dir, file)
            print(f"\nValidating {file}...")
            
            validation_results, df = validate_data(file_path)
            
            if validation_results and df is not None:
                all_validation_results.extend(validation_results)
                
                # Save validated data
                validated_path = os.path.join(validated_dir, file)
                df.to_csv(validated_path, index=False)
                print(f"✓ Validated data saved to: {validated_path}")
                
                # Log validation summary
                failed_checks = [r for r in validation_results if r['validation_status'] == 'FAIL']
                if failed_checks:
                    print(f"✗ {len(failed_checks)} validation issues found in {file}")
                else:
                    print(f"✓ All validation checks passed for {file}")
        
        # Generate comprehensive quality report
        if all_validation_results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(reports_dir, f"data_quality_report_{timestamp}.csv")
            
            if generate_quality_report(all_validation_results, report_path):
                print(f"✓ Data quality report saved: {report_path}")
            else:
                print("✗ Failed to generate quality report")
        
        # Summary
        print("\n" + "="*50)
        print("VALIDATION COMPLETED")
        print("="*50)
        print(f"Validated files: {len(raw_files)}")
        print(f"Validation reports: {reports_dir}")
        print(f"Validated data: {validated_dir}")
        
        logging.info("Data validation process completed")