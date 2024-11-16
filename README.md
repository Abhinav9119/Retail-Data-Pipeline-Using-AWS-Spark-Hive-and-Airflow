# Automated-Data-Pipeline-Leveraging-Aws-Pyspark-hive-and-Airflow-for-Dynamic-Data-Warehousing

### Project Overview
   This project demonstrates the complete lifecycle of a data engineering pipeline, from data extraction to transformation and loading (ETL). The project uses AWS, PySpark, Hive, and Airflow to build a scalable and optimized data warehouse architecture.
    The goal is to extract sales, customer, employee, and product data from AWS storage, perform necessary transformations using PySpark, and load it into a Hive data warehouse. The data is modeled using star schema and separated into different data marts for easy access by various departments. Additionally, it supports incremental loads, slowly changing dimensions (SCDs), and includes mechanisms for logging, error handling, and password encryption.

### Technologies Used
**AWS** S3: Data storage for raw data (CSV and JSON formats)
**PySpark**: Data transformation, cleaning, and normalization
**Hive**: Data warehouse solution for structured storage
**Airflow**: ETL automation and scheduling
**Python**: Primary programming language used
**Dimensional Modeling**: Data warehouse schema designed using star schema
**Security**: Password and key encryption using cryptography library
Logging: Logs generated for tracking ETL pipeline status and errors
Project Flow Diagram

(Add this diagram by uploading the image to the GitHub repo and referencing the image file here.)
![Alt text]([flow_diagram/project flow diagram.png](https://github.com/Abhinav9119/Automated-Data-Pipeline-Leveraging-Aws-Pyspark-hive-and-Airflow-for-Dynamic-Data-Warehousing/blob/main/flow_diagram/project%20flow%20diagram.png))


ETL Pipeline Structure
1. Data Extraction
Extract data from AWS S3, which includes:
Customer Data: JSON format
Employee Data: JSON format
Product Data: JSON format
Sales Data: CSV format
2. Data Transformation
PySpark is used for transforming data:
Data Cleaning: Handle missing or corrupted values.
Normalization: Convert the data into a consistent format.
Handling Slowly Changing Dimensions (SCD): Managing historical data for customer and employee dimensions.
Joining: Joining sales data with customer, product, and employee dimensions to create a fact table.
3. Data Loading
Load the transformed data into Hive:
Fact Table: Sales fact table
Dimension Tables: Customer, Employee, Product, Date
Store data into specific Data Marts for different departments:
Customer Data Mart
Sales Data Mart
Employee Data Mart
4. Automation with Airflow
The entire ETL process is automated using Apache Airflow. This allows for scheduled daily data processing and supports incremental updates.
Airflow DAG manages:
Data extraction
Transformation and loading
Data mart creation
Data Warehouse Schema
The data warehouse is designed in a star schema with the following tables:

Fact Table
Sales Fact Table: Contains sales transaction details, product information, and associated customers and employees.
Dimension Tables
Customer Dimension: Contains customer details like name, age, location, and loyalty tier.
Employee Dimension: Contains employee details such as name, department, and hire date.
Product Dimension: Contains product-related data like product name and category.
Date Dimension: Stores the date of the sales transaction.
Data Marts
Data marts are designed for specific departments. These include:

Customer Data Mart
Customer ID, Name, and Loyalty Tier
Sales Data Mart
Sales ID, Product ID, Customer ID, Sales Amount
Employee Data Mart
Employee ID, Name, Department
Project Directory Structure
plaintext
Copy code
├── data_generation/             
│   ├── DataGenerator.py          # Scripts for generating dummy data in CSV and JSON formats
├── etl/                         
│   ├── ETLProcess.py             # Core ETL process including extraction, transformation, and loading
├── airflow/                     
│   ├── dags/                    
│   │   └── etl_pipeline.py       # Airflow DAG for running the ETL pipeline
├── hive/                        
│   ├── create_tables.hql         # HiveQL scripts to create fact and dimension tables
│   └── data_marts.hql            # HiveQL scripts to create data marts
├── utils/                       
│   ├── Logger.py                 # Logging setup for ETL processes
│   └── Encryption.py             # Password encryption using the cryptography library
├── main.py                       # Main entry point for running the ETL process
├── README.md                     # Project description and instructions
Installation and Setup
1. Prerequisites
Python 3.x
Apache Spark
Apache Hive
Apache Airflow
AWS Account with S3 buckets for storing data
2. Install Dependencies
Install the required Python libraries:
bash
Copy code
pip install pyspark cryptography airflow
3. Setup Hive and AWS
Setup Hive tables using the provided HiveQL scripts in the hive/ directory.
Configure AWS credentials to access S3 for data storage.
4. Run the ETL Pipeline
To manually run the ETL pipeline, execute:
bash
Copy code
python main.py
To schedule and automate the pipeline, set up Apache Airflow, and deploy the DAG in airflow/dags/etl_pipeline.py.
Security Features
Password Encryption: All sensitive information such as passwords and keys are encrypted using the cryptography package.
Logging: Comprehensive logging is implemented to track each step of the ETL pipeline, including any errors.
Project Features
Incremental Load Support: The pipeline handles incremental data loads, only processing new or changed records.
Slowly Changing Dimensions: The pipeline supports SCDs for dimension tables like Customer and Employee to preserve historical data.
Data Marts: Specific data marts are created for departments such as Sales, Customer, and Employee to optimize query performance.
Scalable: The pipeline can easily scale to handle large datasets using PySpark and AWS.
Contributing
Feel free to fork this repository and contribute by submitting a pull request. Any improvements, bug fixes, or additional features are welcome!
