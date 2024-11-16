
import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from pyspark.sql import functions as F

class SchemaValidator:
    # Define expected schema for each data file
    EXPECTED_SCHEMAS = {
        'customer_data.csv': StructType([
            StructField("customer_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("registration_date", DateType(), True)
        ]),
        'product_data.csv': StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("supplier_name", StringType(), True),
            StructField("supplier_contact", StringType(), True)
        ]),
        'sales_data.csv': StructType([
            StructField("sale_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("sale_date", DateType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", FloatType(), True),
            StructField("payment_method", StringType(), True)
        ]),
        'employee_data.csv': StructType([
            StructField("employee_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("department", StringType(), True),
            StructField("position", StringType(), True),
            StructField("salary", FloatType(), True),
            StructField("hire_date", DateType(), True)
        ])
    }

    def __init__(self, spark: SparkSession, file_path: str):
        self.spark = spark
        self.file_path = file_path
        self.data = None
        self.file_extension = self._get_file_extension()
        self.error_directory = "error/"

    def _get_file_extension(self):
        """Get the file extension to decide if it's a CSV or JSON"""
        return self.file_path.split('.')[-1].lower()

    def load_data(self):
        """Load data from file based on file extension (CSV)"""
        file_name = os.path.basename(self.file_path)
        if self.file_extension == 'csv':
            if file_name in SchemaValidator.EXPECTED_SCHEMAS:
                self.data = self.spark.read.schema(SchemaValidator.EXPECTED_SCHEMAS[file_name]).csv(self.file_path, header=True)
            else:
                print(f"Unsupported file format for {file_name}.")
        else:
            print(f"Unsupported file format: {self.file_extension}")

    def move_to_error(self):
        """Moves the file to the error directory if validation fails"""
        if not os.path.exists(self.error_directory):
            os.makedirs(self.error_directory)
        
        file_name = os.path.basename(self.file_path)
        destination = os.path.join(self.error_directory, file_name)
        shutil.move(self.file_path, destination)
        print(f"Moved invalid file {file_name} to {self.error_directory}")

    def validate_schema(self):
        """Validate the schema based on the loaded data"""
        self.load_data()

        if self.data is None:
            print(f"Failed to load data from {self.file_path}.")
            self.move_to_error()
            return

        expected_schema = SchemaValidator.EXPECTED_SCHEMAS.get(os.path.basename(self.file_path), None)

        if expected_schema is None:
            print(f"No expected schema found for {self.file_path}")
            self.move_to_error()
            return

        # Check for missing columns
        for column in expected_schema.names:
            if column not in self.data.columns:
                print(f"Missing column '{column}' in {self.file_path}")
                self.move_to_error()
                return

        # Check data types and null values
        for field in expected_schema.fields:
            column = field.name
            expected_type = field.dataType

            # Check for null values
            null_count = self.data.filter(F.col(column).isNull()).count()

            if null_count > 0:
                print(f"Null values found in column '{column}' in {self.file_path}: {null_count} null(s).")
                self.move_to_error()
                return

            # Check data types
            incorrect_type_count = self.data.filter(~F.col(column).cast(expected_type).isNotNull()).count()

            if incorrect_type_count > 0:
                print(f"Incorrect type for column '{column}' in {self.file_path}. Expected {expected_type}. Found {incorrect_type_count} incorrect type(s).")
                self.move_to_error()
                return

        print(f"{self.file_path} is valid file")
