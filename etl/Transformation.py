from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from etl.SchemaValidator import SchemaValidator






class Transformation:

    


    def __init__(self, spark: SparkSession):
        self.spark = spark

        self.dim_customer_table = spark.createDataFrame([], StructType([
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
    
        self.dim_product_table = spark.createDataFrame([], StructType([
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


    def clean_data(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Ensure columns have appropriate data types and handle nulls."""
        for field in schema.fields:
            col_name = field.name
            col_type = field.dataType

            # Fill missing values with default based on data type
            if isinstance(col_type, StringType):
                df = df.fillna({col_name: 'Unknown'})
            elif isinstance(col_type, FloatType):
                df = df.fillna({col_name: 0.0})
            elif isinstance(col_type, IntegerType):
                df = df.fillna({col_name: 0})
            elif isinstance(col_type, DateType):
                df = df.fillna({col_name: '1970-01-01'})

            # Cast columns to the expected data type
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        
        return df

    def enrich_sales_data(self, sales_df: DataFrame, customer_df: DataFrame, product_df: DataFrame) -> DataFrame:
        """Enrich sales data by joining with customer and product data."""
        enriched_df = sales_df.join(customer_df, on="customer_id", how="left") \
                              .join(product_df, on="product_id", how="left")
        
        return enriched_df

    def derive_columns(self, df: DataFrame) -> DataFrame:
        """Add calculated columns, if needed."""
        if "quantity" in df.columns and "price" in df.columns:
            df = df.withColumn("total_sales_amount", col("quantity") * col("price"))
        return df

    def apply_scd_type_2(self, new_data: DataFrame, dim_table: DataFrame, key_columns: list, update_columns: list) -> DataFrame:
        """
        Implements Slowly Changing Dimension Type 2 by updating existing records in the dimension table.
        Assumes columns: effective_date, end_date, and is_current flag.
        If dim_table is empty (initial load), add new data with current SCD fields.
        """
        # Step 1: If dim_table is empty (initial load), we will add the new data with SCD columns
        if dim_table.count() == 0:
            # Assign the current date for all records and mark them as current
            new_data = new_data.withColumn("effective_date", current_date()) \
                               .withColumn("end_date", lit(None)) \
                               .withColumn("is_current", lit(True))
            return new_data

        # Step 2: For subsequent loads, apply SCD Type 2
        cond = [dim_table[key] == new_data[key] for key in key_columns]
        for col_name in update_columns:
            cond.append(dim_table[col_name] != new_data[col_name])  # Check for changes in the columns to update

        # Create updated records by ending current records and adding new ones
        expired_records = dim_table.join(new_data, cond, "inner") \
                                   .withColumn("end_date", current_date()) \
                                   .withColumn("is_current", lit(False))  # Mark old records as inactive

        # Step 3: Create new records with current data
        new_active_records = new_data.withColumn("effective_date", current_date()) \
                                     .withColumn("end_date", lit(None)) \
                                     .withColumn("is_current", lit(True))  # Add new active records

        # Step 4: Union the updated records with new data and retain unchanged records
        unchanged_records = dim_table.filter(~dim_table[key_columns[0]].isin(expired_records[key_columns[0]].collect()))

        # Combine all: unchanged records, expired records, and new active records
        result = unchanged_records.unionByName(expired_records).unionByName(new_active_records)
        return result

    def transform(self, customer_df: DataFrame, employee_df: DataFrame, sales_df: DataFrame, product_df: DataFrame) -> dict:
        """Apply transformations to each DataFrame."""
        # Clean data for each DataFrame
        customer_df = self.clean_data(customer_df, SchemaValidator.EXPECTED_SCHEMAS['customer_data.csv'])
        employee_df = self.clean_data(employee_df, SchemaValidator.EXPECTED_SCHEMAS['employee_data.csv'])
        sales_df = self.clean_data(sales_df, SchemaValidator.EXPECTED_SCHEMAS['sales_data.csv'])
        product_df = self.clean_data(product_df, SchemaValidator.EXPECTED_SCHEMAS['product_data.csv'])

        # Enrich and transform sales data
        sales_df = self.enrich_sales_data(sales_df, customer_df, product_df)
        sales_df = self.derive_columns(sales_df)

        # Apply SCD Type 2 transformations (for customer and product)
        customer_df = self.apply_scd_type_2(customer_df, self.dim_customer_table, ["customer_id"], ["first_name", "last_name", "email", "address"])
        product_df = self.apply_scd_type_2(product_df, self.dim_product_table, ["product_id"], ["product_name", "category", "price", "supplier_name"])

        # Return transformed data in star schema format
        star_schema = {
            "fact_sales": sales_df.select("sale_id", "customer_id", "product_id", "sale_date", "quantity", "total_sales_amount", "payment_method"),
            "dim_customer": customer_df.select("customer_id", "first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code", "registration_date", "effective_date", "end_date", "is_current"),
            "dim_product": product_df.select("product_id", "product_name", "category", "price", "stock_quantity", "supplier_name", "supplier_contact", "effective_date", "end_date", "is_current"),
            "dim_employee": employee_df.select("employee_id", "first_name", "last_name", "email", "department", "position", "salary", "hire_date")
        }

        return star_schema



