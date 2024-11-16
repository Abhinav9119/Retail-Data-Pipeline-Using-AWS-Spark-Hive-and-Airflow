
from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name="Retail Data Pipeline", enable_hive=True):
        self.app_name = app_name
        self.enable_hive = enable_hive
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        # Initialize Spark session builder
        spark_builder = SparkSession.builder.appName(self.app_name)
        
        # Enable Hive support if specified
        if self.enable_hive:
            spark_builder = spark_builder.config("spark.sql.catalogImplementation", "hive") \
                                         .config("spark.sql.hive.convertMetastoreParquet", "true") \
                                         .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
                                         .config("spark.hadoop.hive.metastore.uris", "thrift://0.0.0.0:9083") \
                                         .enableHiveSupport()
        
        # Build and return the Spark session
        return spark_builder.getOrCreate()

    def get_spark_session(self):
        return self.spark

    def stop_session(self):
        self.spark.stop()











# spark = SparkSession.builder \
#     .appName("Retail Data Pipeline") \
#     .config("spark.sql.catalogImplementation", "hive") \
#     .config("spark.sql.hive.convertMetastoreParquet", "true") \
#     .enableHiveSupport() \
#     .getOrCreate()
