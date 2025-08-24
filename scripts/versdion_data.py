import os
import subprocess
import logging
import sys
from datetime import datetime
import hashlib

# Get project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Set up logging
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'versioning.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_dvc_initialized():
    """Check if DVC is initialized in the project"""
    try:
        result = subprocess.run(['dvc', 'status'], capture_output=True, text=True, cwd=project_root)
        return os.path.exists(os.path.join(project_root, '.dvc'))
    except subprocess.CalledProcessError:
        logging.error("DVC not initialized in the project")
        return False

def initialize_dvc():
    """Initialize DVC in the project if not already initialized"""
    try:
        if not check_dvc_initialized():
            subprocess.run(['dvc', 'init'], check=True, capture_output=True, text=True, cwd=project_root)
            logging.info("DVC initialized in the project")
            subprocess.run(['git', 'add', '.dvc', '.gitignore'], check=True, cwd=project_root)
            subprocess.run(['git', 'commit', '-m', 'Initialize DVC'], check=True, cwd=project_root)
            print("✓ DVC initialized successfully")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error initializing DVC: {e.stderr}")
        print(f"✗ Error initializing DVC: {e.stderr}")
        return False

def get_file_checksum(file_path):
    """Calculate MD5 checksum of a file"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.warning(f"Error calculating checksum for {file_path}: {str(e)}")
        return None

def version_data():
    """Version data using DVC"""
    try:
        # Change to project directory
        os.chdir(project_root)
        
        # Initialize DVC if not already initialized
        if not initialize_dvc():
            return False
        
        # Add data to DVC tracking
        data_dirs = ['data/raw', 'data/processed', 'data/validated']
        versioned_files = []
        
        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                # Get list of files in directory
                for root, _, files in os.walk(data_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        result = subprocess.run(['dvc', 'add', file_path], capture_output=True, text=True, cwd=project_root)
                        if result.returncode == 0:
                            checksum = get_file_checksum(file_path)
                            versioned_files.append((file_path, checksum))
                            logging.info(f"Successfully versioned {file_path}")
                            print(f"✓ Versioned {file_path}")
                        else:
                            logging.error(f"Error versioning {file_path}: {result.stderr}")
                            print(f"✗ Error versioning {file_path}")
                
                # Add .dvc files to Git
                subprocess.run(['git', 'add', f"{data_dir}/*.dvc"], check=True, cwd=project_root)
        
        # Commit changes to Git
        commit_message = f"Version data: {datetime.now().strftime('%Y%m%d_%H%M%S')}"
        subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=project_root)
        logging.info("Git commit created for versioned data")
        print("✓ Git commit created for versioned data")
        
        # Generate version report
        generate_version_report(versioned_files)
        
        logging.info("Data versioning completed successfully")
        print("✓ Data versioning completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in data versioning: {e.stderr}")
        print(f"✗ Data versioning failed: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Error in data versioning: {str(e)}")
        print(f"✗ Data versioning failed: {str(e)}")
        return False

def generate_version_report(versioned_files):
    """Generate versioning report"""
    try:
        reports_dir = os.path.join(project_root, 'data', 'version_reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(reports_dir, f'version_report_{timestamp}.txt')
        
        # Get DVC status
        result = subprocess.run(['dvc', 'status'], capture_output=True, text=True, cwd=project_root)
        
        with open(report_path, 'w') as f:
            f.write(f"Data Versioning Report\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
            f.write("=" * 50 + "\n\n")
            f.write("DVC Status:\n")
            f.write(result.stdout if result.returncode == 0 else result.stderr)
            f.write("\n" + "=" * 50 + "\n")
            f.write("Versioned Files:\n")
            for file_path, checksum in versioned_files:
                f.write(f"- {file_path} (MD5: {checksum or 'N/A'})\n")
            f.write("\nVersioned Directories:\n")
            f.write("- data/raw/\n")
            f.write("- data/processed/\n")
            f.write("- data/validated/\n")
            f.write("\nSource Information:\n")
            f.write("- Raw data: Original customer and transaction datasets\n")
            f.write("- Processed data: Cleaned and transformed datasets from transform_data.py\n")
            f.write("- Validated data: Validated datasets from validation scripts\n")
        
        logging.info(f"Version report generated: {report_path}")
        print(f"✓ Version report generated: {report_path}")
        
    except Exception as e:
        logging.error(f"Error generating version report: {str(e)}")
        print(f"✗ Error generating version report: {str(e)}")

if __name__ == "__main__":
    logging.info("Starting data versioning process")
    print("Starting data versioning process...")
    
    success = version_data()
    
    if success:
        print("✓ Data versioning completed successfully")
    else:
        print("✗ Data versioning failed")