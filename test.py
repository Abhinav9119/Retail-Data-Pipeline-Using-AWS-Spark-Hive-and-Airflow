
# from util.config import *
# from download.S3Downloader import S3Downloader
# # from pyspark.sql import SparkSession

# # # Initialize Spark session with AWS credentials
# # spark = SparkSession.builder \
# #     .appName("S3 to HDFS Transfer") \
# #     .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
# #     .config("spark.hadoop.fs.s3a.secret.key", aws_secrete_key) \
# #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
# #     .getOrCreate()

# # # Define S3 and HDFS paths
# # s3_path = "s3a://big-data-a/books.csv"  # S3 file path
# # hdfs_path = "hdfs://localhost:9870/user/project/data/books.csv"  # HDFS destination path

# # # Read the file from S3
# # df = spark.read.csv(s3_path,header=True,inferSchema=True)
# # df.show()
# # # Write the DataFrame to HDFS
# # df.write.csv(hdfs_path)

# # print(f'Successfully transferred {s3_path} from S3 to {hdfs_path} in HDFS.')

# # # Stop the Spark session
# # spark.stop()


# # from pyspark.sql import SparkSession

# # # Initialize Spark session with AWS credentials
# # spark = SparkSession.builder \
# #     .appName("S3 to HDFS Transfer") \
# #     .config("spark.hadoop.fs.s3a.access.key",aws_access_key) \
# #     .config("spark.hadoop.fs.s3a.secret.key", aws_secrete_key) \
# #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
# #     .getOrCreate()

# # # Define S3 path
# # s3_path = "s3a://big-data-a/books.csv"

# # # Read the CSV file from S3
# # try:
# #     df = spark.read.csv(s3_path, header=True, inferSchema=True)
# #     df.show()
# # except Exception as e:
# #     print(f"Error reading from S3: {e}")

# # # Stop the Spark session
# # spark.stop()



# # import subprocess

# # def copy_s3_to_hdfs(s3_path, hdfs_path):
# #     """
# #     Copy files from S3 to HDFS using Hadoop's distcp command.

# #     :param s3_path: str, the S3 path (e.g., s3a://bucket-name/path/)
# #     :param hdfs_path: str, the destination HDFS path (e.g., hdfs://namenode:8020/path/)
# #     """
# #     command = [
# #         'hadoop', 'distcp',
# #         s3_path,
# #         hdfs_path
# #     ]

# #     try:
# #         # Execute the distcp command
# #         subprocess.run(command, check=True)
# #         print(f'Successfully copied from {s3_path} to {hdfs_path}')
# #     except subprocess.CalledProcessError as e:
# #         print(f'Error occurred while copying: {e}')

# # if __name__ == '__main__':
# #     # Define your S3 and HDFS paths
# #     s3_bucket = "s3a://big-data-a/books.csv" # Use * for all files in the directory
# #     hdfs_destination = 'hdfs://localhost:8020/user/project/data/'

# #     # Call the function to copy files
# #     copy_s3_to_hdfs(s3_bucket, hdfs_destination)


# # import boto3
# # import s3fs
# # import pydoop.hdfs as hdfs

# # # AWS credentials
# # aws_access_key_id = aws_access_key
# # aws_secret_access_key = aws_secrete_key

# # # S3 and HDFS paths
# # bucket_name = 'big-data-a'
# # s3_file_key = 'books.csv'  # Key (path) of the file in S3
# # hdfs_path = '/user/project/data/'  # Destination path in HDFS

# # # Initialize boto3 S3 client with AWS credentials
# # s3_client = boto3.client(
# #     's3',
# #     aws_access_key_id=aws_access_key_id,
# #     aws_secret_access_key=aws_secret_access_key
# # )

# # # Initialize s3fs with the same AWS credentials
# # s3_fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

# # # Full path to the S3 file
# # s3_path = f"s3://{bucket_name}/{s3_file_key}"

# # # Copy file from S3 to HDFS
# # try:
# #     with s3_fs.open(s3_path, 'rb') as s3_file:
# #         with hdfs.open(hdfs_path, 'wb') as hdfs_file:
# #             hdfs_file.write(s3_file.read())
# #     print(f"File from {s3_path} has been copied to HDFS at {hdfs_path}")
# # except Exception as e:
# #     print(f"Error: {e}")




# # from pyspark.sql import SparkSession

# # class SparkSessionManager:
# #     def __init__(self, app_name='RetailDataPipeline'):
# #         self.app_name = app_name
# #         self.spark = None

# #     def get_session(self):
# #         if not self.spark:
# #             self.spark = SparkSession.builder \
# #                 .appName(self.app_name) \
# #                 .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
# #                 .config("spark.hadoop.fs.s3a.secret.key", aws_secrete_key) \
# #                 .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
# #                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
# #                 .config("spark.hadoop.fs.s3a.path.style.access", "true") \
# #                 .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
# #                 .getOrCreate()
# #         return self.spark

# # def download_from_s3_to_hdfs(spark, s3_path, hdfs_path):
# #     # Read data from S3
# #     df = spark.read.csv(s3_path, header=True, inferSchema=True)
    
# #     # Write data to HDFS
# #     df.write.mode("overwrite").parquet(hdfs_path)
# #     print(f"Data from {s3_path} written to {hdfs_path} on HDFS")

# # if __name__ == "__main__":
# #     spark_manager = SparkSessionManager()
# #     spark = spark_manager.get_session()

# #     # Define S3 path and HDFS path
# #     s3_path = "s3a://big-data-a/books.csv"
# #     hdfs_path = "user/project/data"

# #     # Download from S3 to HDFS
# #     download_from_s3_to_hdfs(spark, s3_path, hdfs_path)



# # import boto3
# # from hdfs import InsecureClient

# # # AWS credentials
# # aws_access_key_id = aws_access_key
# # aws_secret_access_key = aws_secrete_key
# # region_name = 'us-east-1'

# # # S3 configuration
# # s3 = boto3.client('s3',
# #                   aws_access_key_id=aws_access_key_id,
# #                   aws_secret_access_key=aws_secret_access_key,
# #                   region_name=region_name)
# # bucket_name = 'big-data-a'
# # prefix = ''

# # # HDFS configuration
# # hdfs_client = InsecureClient('hdfs://localhost:9870', user='abhinavmk')
# # hdfs_path = '/user/project/data/'

# # # Function to download and upload files
# # def download_and_upload_s3_to_hdfs():
# #     # List objects in the S3 bucket
# #     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
# #     for obj in response.get('Contents', []):
# #         s3_key = obj['Key']
# #         local_file = '/tmp/' + s3_key.split('/')[-1]

# #         # Download the file from S3
# #         s3.download_file(bucket_name, s3_key, local_file)

# #         # Upload the file to HDFS
# #         with open(local_file, 'rb') as f:
# #             hdfs_client.write(hdfs_path + s3_key.split('/')[-1], f, overwrite=True)

# #         print(f"Copied {s3_key} to HDFS")

# # if __name__ == "__main__":
# #     download_and_upload_s3_to_hdfs()


# # import boto3
# # from hdfs import InsecureClient

# # # AWS credentials
# # aws_access_key_id = 'your_access_key_id'
# # aws_secret_access_key = 'your_secret_access_key'
# # region_name = 'us-east-1'

# # # S3 configuration
# # s3 = boto3.client('s3',
# #                   aws_access_key_id=aws_access_key_id,
# #                   aws_secret_access_key=aws_secret_access_key,
# #                   region_name=region_name)
# # bucket_name = 'big-data-a'
# # prefix = ''

# # # HDFS configuration
# # # Replace with the WebHDFS URL of your NameNode. Make sure the WebHDFS service is enabled.
# # hdfs_client = InsecureClient('http://localhost:9870/webhdfs/v1', user='abhinavmk')
# # hdfs_path = '/user/project/data/'

# # # Function to download and upload files
# # def download_and_upload_s3_to_hdfs():
# #     # List objects in the S3 bucket
# #     response = s3.list_objects_v2(Bucket=bucket_name)
    
# #     for obj in response.get('Contents', []):
# #         s3_key = obj['Key']
# #         local_file = '/tmp/' + s3_key.split('/')[-1]

# #         # Download the file from S3
# #         s3.download_file(bucket_name, s3_key, local_file)

# #         # Upload the file to HDFS
# #         with open(local_file, 'rb') as f:
# #             hdfs_client.write(hdfs_path + s3_key.split('/')[-1], f, overwrite=True)

# #         print(f"Copied {s3_key} to HDFS")

# # if __name__ == "__main__":
# #     download_and_upload_s3_to_hdfs()


# # import boto3
# # from hdfs import InsecureClient
# # import os

# # # AWS credentials
# # aws_access_key_id = aws_access_key
# # aws_secret_access_key = aws_secrete_key
# # region_name = 'us-east-1'

# # # S3 configuration
# # s3 = boto3.client('s3',
# #                   aws_access_key_id=aws_access_key_id,
# #                   aws_secret_access_key=aws_secret_access_key,
# #                   region_name=region_name)
# # bucket_name = 'big-data-a'
# # prefix = ''

# # # HDFS configuration
# # hdfs_client = InsecureClient('http://localhost:9870/webhdfs/v1', user='abhinavmk')
# # hdfs_path = '/user/project/data/'

# # # Temporary directory for downloads
# # local_temp_dir = '/home/abhinavmk/staging_temp/'

# # # Ensure the local temporary directory exists
# # if not os.path.exists(local_temp_dir):
# #     os.makedirs(local_temp_dir)

# # # Function to download and upload files
# # def download_and_upload_s3_to_hdfs():
# #     # List objects in the S3 bucket
# #     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
# #     for obj in response.get('Contents', []):
# #         s3_key = obj['Key']
# #         local_file = os.path.join(local_temp_dir, s3_key.split('/')[-1])

# #         # Download the file from S3
# #         s3.download_file(bucket_name, s3_key, local_file)

# #         # Upload the file to HDFS
# #         with open(local_file, 'rb') as f:
# #             hdfs_client.write(hdfs_path + s3_key.split('/')[-1], f, overwrite=True)

# #         print(f"Copied {s3_key} to HDFS")

# # if __name__ == "__main__":
# #     download_and_upload_s3_to_hdfs()






# # import boto3
# # from hdfs import InsecureClient
# # import os

# # # AWS credentials
# # aws_access_key_id = aws_access_key
# # aws_secret_access_key = aws_secrete_key
# # region_name = 'us-east-1'

# # # S3 configuration
# # s3 = boto3.client('s3',
# #                   aws_access_key_id=aws_access_key_id,
# #                   aws_secret_access_key=aws_secret_access_key,
# #                   region_name=region_name)
# # bucket_name = 'big-data-a'
# # prefix = 'myfolder'

# # # HDFS configuration
# # hdfs_client = InsecureClient('http://localhost:9870/webhdfs/v1', user='abhinavmk')
# # hdfs_path = '/user/project/data/'

# # # Temporary directory for downloads
# # local_temp_dir = '/home/abhinavmk/staging_temp/'

# # # Ensure the local temporary directory exists
# # if not os.path.exists(local_temp_dir):
# #     os.makedirs(local_temp_dir)

# # # Function to download and upload files
# # def download_and_upload_s3_to_hdfs():
# #     # List objects in the S3 bucket
# #     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
# #     for obj in response.get('Contents', []):
# #         s3_key = obj['Key']
# #         file_name = s3_key.split('/')[-1]
        
# #         # Skip any unexpected hidden files or directories
# #         if not file_name or file_name.startswith('.'):
# #             continue

# #         local_file = os.path.join(local_temp_dir, file_name)

# #         # Download the file from S3
# #         s3.download_file(bucket_name, s3_key, local_file)

# #         # Upload the file to HDFS
# #         with open(local_file, 'rb') as f:
# #             hdfs_client.write(hdfs_path + file_name, f, overwrite=True)

# #         print(f"Copied {s3_key} to HDFS")

# # if __name__ == "__main__":
# #     download_and_upload_s3_to_hdfs()

# #----------------------------------------------------------------------------------------------------------------------------------------------------------------




# # final extraction code


# # import boto3
# # from hdfs import InsecureClient
# # import os

# # # AWS credentials
# # aws_access_key_id =aws_access_key
# # aws_secret_access_key = aws_secrete_key
# # region_name = 'us-east-1'

# # # S3 configuration
# # s3 = boto3.client('s3',
# #                   aws_access_key_id=aws_access_key_id,
# #                   aws_secret_access_key=aws_secret_access_key,
# #                   region_name=region_name)
# # bucket_name = 'big-data-a'
# # prefix = ''

# # # HDFS configuration
# # hdfs_client = InsecureClient('http://localhost:9870/webhdfs/v1', user='abhinavmk')
# # hdfs_path = '/user/project/data'

# # # Temporary directory for downloads
# # local_temp_dir = '/home/abhinavmk/Desktop/project/retail_datawarehousing/staging_temp/'

# # # Ensure the local temporary directory exists
# # if not os.path.exists(local_temp_dir):
# #     os.makedirs(local_temp_dir)

# # # Function to download and upload files
# # def download_and_upload_s3_to_hdfs():
# #     # List objects in the S3 bucket
# #     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
# #     for obj in response.get('Contents', []):
# #         s3_key = obj['Key']
# #         file_name = s3_key.split('/')[-1]
        
# #         # Skip any unexpected hidden files or directories
# #         if not file_name or file_name.startswith('.'):
# #             print(f"Skipping hidden or empty file: {file_name}")
# #             continue

# #         local_file = os.path.join(local_temp_dir, file_name)

# #         # Download the file from S3
# #         print(f"Downloading {s3_key} from S3 to {local_file}")
# #         s3.download_file(bucket_name, s3_key, local_file)

# #         # Upload the file to HDFS
# #         local_file_path = '/home/abhinavmk/Desktop/project/retail_datawarehousing/staging_temp/'  # Replace with the path to your local file
# #         hdfs_file_name = 'books.csv'  # Name of the file once uploaded to HDFS

# # # Upload file to HDFS
# #         try:
# #             with open(local_temp_dir + hdfs_file_name , 'rb') as local_file:
# #                 hdfs_client.write(hdfs_path + hdfs_file_name, local_file, overwrite=True)
# #             print(f"File {local_temp_dir } successfully uploaded to HDFS at {hdfs_path}{hdfs_file_name}")
# #         except Exception as e:
# #             print(f"Failed to upload file to HDFS: {e}")






# # if __name__ == "__main__":
# #     download_and_upload_s3_to_hdfs()






# import boto3
# import traceback
# import os
# #from src.main.utility.logging_config import *

# class S3FileDownloader:
#     def __init__(self,s3_client, bucket_name, local_directory):
#         self.bucket_name = bucket_name
#         self.local_directory = local_directory
#         self.s3_client = s3_client

#     def download_files(self, list_files):
#         #logger.info("Running download files for these files %s",list_files)
#         for key in list_files:
#             file_name = os.path.basename(key)
#             #logger.info("File name %s ",file_name)
#             download_file_path = os.path.join(self.local_directory, file_name)
#             try:
#                 self.s3_client.download_file(self.bucket_name,key,download_file_path)
#             except Exception as e:
#                 error_message = f"Error downloading file '{key}': {str(e)}"
#                 traceback_message = traceback.format_exc()
#                 print(error_message)
#                 print(traceback_message)
#                 raise e
            

# if __name__=="__main__":
# #     import boto3
# # from src.main.utility.logging_config import logger  # Ensure logging is configured

# # # Initialize the S3 client
# # s3_client = boto3.client(
# #     's3',
# #     aws_access_key_id=aws_access_key,
# #     aws_secret_access_key=aws_secrete_key,
# #     region_name=region
# # )

# # # Define your S3 bucket name and local download directory
# # bucket_name = aws_bucket_name
# # local_directory = '/path/to/local/directory'

# # # List of files to download (these are the S3 keys, including folder paths)
# # list_files = [
# #     'customer/customer_data.csv',
# #     'product/product_data.json',
# #     'employee/employee_data.csv',
# #     'sales/sales_data.csv'
# # ]

# # # Create an instance of S3FileDownloader
# # downloader = S3FileDownloader(s3_client, bucket_name, local_directory)

# # # Call the method to download files
# # downloader.download_files(list_files)

#     import boto3
#     #from src.main.utility.logging_config import logger  # Ensure logging is configured

# # Initialize the S3 client
#     s3_client = boto3.client(
#         's3',
#         aws_access_key_id=aws_access_key,
#         aws_secret_access_key=aws_secrete_key,
#         region_name=region
#     )

# bucket_name = aws_bucket_name

# #download customer data
# # Define bucket name and local base directory
#     local_directory = customer_dir  # Local directory for the downloaded file
# # Specify the exact file you want to download from the customer folder in S3
#     aws_dir = ['customer/customer_data.csv']

# # Create an instance of S3FileDownloader
#     downloader = S3FileDownloader(s3_client, bucket_name, local_directory)

# # Download just the specified file
#     downloader.download_files(aws_dir)


# local_directory = customer_dir
# aws_dir =
# downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
# downloader.download_files(aws_dir)





# from pyspark.sql import SparkSession

# class SimpleSparkSession:
#     def __init__(self, app_name="SimpleApp", enable_hive=False):
#         self.app_name = app_name
#         self.enable_hive = enable_hive
#         self.spark = self._create_spark_session()

#     def _create_spark_session(self):
#         # Initialize Spark session builder
#         spark_builder = SparkSession.builder.appName(self.app_name)
        
#         # Enable Hive support if specified
#         if self.enable_hive:
#             spark_builder = spark_builder.enableHiveSupport()
        
#         # Build and return the Spark session
#         return spark_builder.getOrCreate()

#     def get_spark_session(self):
#         return self.spark

#     def stop_session(self):
#         self.spark.stop()


# def read_from_hive(self, database, table):
#         # Set the database (optional)
#         self.spark.sql(f"USE {database}")
        
#         # Load data from the specified Hive table
#         df = self.spark.sql(f"SELECT * FROM {table}")
        
#         return df

# if __name__=="__main__":
#     # Create Spark session instance
#     spark_manager = SimpleSparkSession(app_name="MyApp", enable_hive=True)

# # Get the Spark session
#     spark = spark_manager.get_spark_session()

# # Use Spark for operations
#     df = spark.read.csv("/home/abhinavmk/Downloads/BigData/data/books.csv", header=True, inferSchema=True)
#     df.show()

# # Stop the Spark session when done
#     spark_manager.stop_session()








