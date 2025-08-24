-- Create database
CREATE DATABASE IF NOT EXISTS mydb;
USE mydb;

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    location VARCHAR(100),
    plan_type VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_date DATE,
    amount DECIMAL(10, 2),
    category VARCHAR(50),
    payment_method VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Customer features table
CREATE TABLE IF NOT EXISTS customer_features (
    customer_id VARCHAR(50) PRIMARY KEY,
    total_spend DECIMAL(15, 2),
    avg_transaction_amount DECIMAL(10, 2),
    transaction_count INT,
    days_since_signup INT,
    days_since_last_transaction INT,
    monthly_spend DECIMAL(15, 2),
    category_diversity INT,
    payment_method_count INT,
    churn_risk_score DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Pipeline metadata table
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    run_id VARCHAR(50) PRIMARY KEY,
    pipeline_name VARCHAR(100),
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);