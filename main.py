#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< imports >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

from util.config import *
import boto3
from download.S3FileDownloader import S3FileDownloader
from util.SparkSessionManager import SparkSessionManager
from etl.SchemaValidator import SchemaValidator
from etl.Extractor import Extractor
from etl.Transformation import Transformation
from etl.DataWarehouseLoader import DataLoader
from util.DeleteFiles import delete_files
# <<<<<<<<<<<<<<<<<<<<<<<<<<< making s3 connection >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

s3_client = boto3.client('s3',aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secrete_key,region_name=region)
bucket_name = aws_bucket_name



#<<<<<<<<<<<<<<<<<<<<<<<<<< download data from S3 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

#download customer data
# Define bucket name and local base directory
local_directory = local_customer_dir # Local directory for the downloaded file
# Specify the exact file you want to download from the customer folder in S3
aws_dir =aws_customer_dir
# Create an instance of S3FileDownloader
print("Downloading Files From S3 ...................................")
downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
# Download just the specified file from customer folder in s3
downloader.download_files(aws_dir)

#download employee_data
local_directory =local_employee_dir
aws_dir =aws_employee_dir
downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
downloader.download_files(aws_dir)

# download product_ data
local_directory =local_product_dir
aws_dir =aws_product_dir
downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
downloader.download_files(aws_dir)

#download sales_data
local_directory =local_sales_dir
aws_dir =aws_sales_dir
downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
downloader.download_files(aws_dir)


#<<<<<<<<<<<<<<<<<<<<<<<<<<< creating spark session >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

print("creating spark session ...............................")

spark_manager = SparkSessionManager(app_name="SchemaValidator", enable_hive=True)
spark = spark_manager.get_spark_session()





#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< schema validation >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

#spark = SparkSession.builder.master("local[1]").appName("SchemaValidator").getOrCreate()

print("validating schmema .........................................")

# Create instances for each file and validate the schema
customer_validator = SchemaValidator(spark, local_customer_dir + "/" + customer_data_file)
customer_validator.validate_schema()

product_validator = SchemaValidator(spark, local_product_dir + "/" + product_data_file)
product_validator.validate_schema()

sales_validator = SchemaValidator(spark, local_sales_dir + "/" + sales_data_file)
sales_validator.validate_schema()

employee_validator = SchemaValidator(spark, local_employee_dir + "/" + employee_data_file)
employee_validator.validate_schema()

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Data Extraction >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

print("start extracting data ..........................................")
# Create Extractor instance
extractor = Extractor(spark)

data_schema=SchemaValidator.EXPECTED_SCHEMAS
local_file_path={customer_data_file:local_customer_dir + "/" + customer_data_file,employee_data_file:local_employee_dir + "/" + employee_data_file,
                 sales_data_file: local_sales_dir + "/" + sales_data_file,product_data_file:local_product_dir + "/" + product_data_file}


#extracting customer data
df_customer=extractor.extract(local_file_path[customer_data_file],data_schema[customer_data_file])

if df_customer:
    print(f"{customer_data_file} file is successfully extracted in spark dataframe")
else:
    print(f"error while file extracting {product_data_file} file ............")

# extracting employee data
df_employee=extractor.extract(local_file_path[employee_data_file],data_schema[employee_data_file])

if df_employee:
    print(f"{employee_data_file} file is successfully extracted in spark dataframe")
else:
    print(f"error while file extracting  {employee_data_file} file............")


#extracting sales data
df_sales=extractor.extract(local_file_path[sales_data_file],data_schema[sales_data_file])

if df_sales:
    print(f"{sales_data_file} file succesfully extracted in spark dataframe")
else:
    print(f"error while file extracting {sales_data_file} file..............")


# extracting product data
df_product=extractor.extract(local_file_path[product_data_file],data_schema[product_data_file])

if df_product:
    print(f"{product_data_file} file successfully extracted in spark dataframe")
else:
    print(f"error while extracting {product_data_file} file................")


#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< transformations >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

print("starting transformations .........................................")

# spark = SparkSession.builder.master("local[1]").appName("Transformation").getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType


    # Initialize empty dimension tables for the first load   (reference for slowly changing dimensions for later loading)
dim_customer_table = spark.createDataFrame([], StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("effective_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("is_current", StringType(), True)
]))
    
dim_product_table = spark.createDataFrame([], StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("supplier_contact", StringType(), True),
    StructField("effective_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("is_current", StringType(), True)
]))

    # Example DataFrames (These would come from your raw data)
    # df_customer = spark.createDataFrame([
    #     ("1", "John", "Doe", "john.doe@example.com", "123-456-7890", "123 Elm St", "Springfield", "IL", "62701", "2020-01-01"),
    #     ("2", "Jane", "Smith", "jane.smith@example.com", "234-567-8901", "456 Oak St", "Chicago", "IL", "60601")
    # ], ["customer_id", "first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code", "registration_date"])

# df_customer = spark.read.schema(EXPECTED_SCHEMAS["customer_data.csv"]).csv("project_data/new_data/customer_data.csv", header=True, inferSchema=False)
# df_employee = spark.read.schema(EXPECTED_SCHEMAS["employee_data.csv"]).csv('project_data/new_data/employee_data.csv', header=True, inferSchema=False)
# df_sales = spark.read.schema(EXPECTED_SCHEMAS["sales_data.csv"]).csv("project_data/new_data/sales_data.csv", header=True, inferSchema=False)
# df_product = spark.read.schema(EXPECTED_SCHEMAS["product_data.csv"]).csv('project_data/new_data/product_data.csv', header=True, inferSchema=False)


    # Initialize Transformation class and run transformation
transformer = Transformation(spark)
transformed_data = transformer.transform(df_customer, df_employee, df_sales,df_product)

if transformed_data:
    print("data is transformed successfully....................................")
else:
    print("error while transforming data......................................")


# # to print transformed data
# print(transformed_data["dim_customer"].show(5))
# print(transformed_data["dim_employee"].show(5))
# print(transformed_data["dim_product"].show(5))
# print(transformed_data["fact_sales"].show(5))

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Load >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# print(transformed_data['fact_sales'].printSchema(),"\n", transformed_data['dim_customer'].printSchema(),"\n", transformed_data['dim_product'].printSchema(),"\n",transformed_data["dim_employee"].printSchema())










print("start loading data ..........................................")

data_loader = DataLoader(spark, "retailwarehouse")


    # Load all dataframes into Hive
data_loader.load_all_dataframes(transformed_data['fact_sales'], transformed_data['dim_customer'], transformed_data['dim_product'],transformed_data["dim_employee"])

    # Check if all dataframes were loaded successfully
if data_loader.all_data_loaded():
    print("All dataframes were loaded successfully.")
else:
    print("Some dataframes failed to load.")


#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Deleting Data From Staging Area >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>




# Example usage
if data_loader.all_data_loaded:
    print("clearing staging area .................................")
    directory_path =local_customer_dir
    delete_files(directory_path, flag=True)

    directory_path =local_employee_dir
    delete_files(directory_path, flag=True)


    directory_path =local_product_dir
    delete_files(directory_path, flag=True)

    directory_path =local_sales_dir
    delete_files(directory_path, flag=True)
    

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< stop spark session >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

print("stopping spark session .............................................")
spark_manager.stop_session()
