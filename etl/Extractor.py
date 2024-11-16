from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

class Extractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, file_path: str, schema: StructType):
        """
        Extract data from a CSV file using the provided schema.

        :param file_path: Path to the CSV file.
        :param schema: The schema to apply to the data (of type StructType).
        :return: Spark DataFrame with the provided schema.
        """
        # Read the CSV file with the provided schema
        try:
            df = self.spark.read.schema(schema).csv(file_path, header=True, inferSchema=False)
            print(f"Data extracted successfully from {file_path}")
            return df
        except Exception as e:
            print(f"Error occurred while extracting data from {file_path}: {e}")
            return None
