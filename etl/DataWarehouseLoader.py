from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from functools import reduce

class DataLoader:
    def __init__(self, spark: SparkSession, hive_database: str):
        self.spark = spark
        self.hive_database = hive_database
        self.spark.sql(f"USE {hive_database}")
        self.load_success = True  # Flag to track if all dataframes are loaded successfully

    def load_fact_sales(self, df_fact_sales):
        """Load or append fact_sales data incrementally into Hive."""
        try:
            table_name = "fact_sales"
            df_fact_sales.write.mode("append").insertInto(table_name)
            print(f"Fact sales data loaded into {table_name} table.")
        except Exception as e:
            self.load_success = False
            print(f"Failed to load fact_sales data into {table_name} table: {e}")

    def _process_scd_type2(self, df_new, table_name, dimension_cols, id_col, effective_col, is_current_col):
        """Process SCD Type 2 logic for incremental loading into any dimension table."""
        try:
            # Load existing data from Hive
            existing_data = self.spark.table(table_name)

            # If the table is empty, insert the new data directly
            if existing_data.count() == 0:
                print(f"{table_name} is empty, loading new data directly.")
                df_new.write.mode("append").insertInto(table_name)
                return

            # Join to detect changes
            updated_data = df_new.join(
                existing_data,
                on=id_col,
                how="left"
            ).select(
                df_new["*"],
                existing_data[effective_col].alias("existing_effective_date"),
                existing_data[is_current_col].alias("existing_is_current")
            )

            # Filter for changes (only consider current records)
            changed_data = updated_data.filter(
                (col("existing_is_current") == lit(True)) &
                (
                    # Check for changes in the dimension columns
                    reduce(lambda a, b: a | b, [
                        df_new[col_name] != existing_data[col_name] for col_name in dimension_cols
                    ])
                )
            )

            # Prepare records to update: set end_date and is_current to False for old records
            updates = changed_data.withColumn("end_date", current_date()).withColumn("is_current", lit(False))

            # Prepare new records: set effective_date and is_current to True for new records
            new_records = changed_data.withColumn("effective_date", current_date()).withColumn("is_current", lit(True))

            # Prepare non-matching records: set effective_date and is_current to True
            non_matching = df_new.join(
                existing_data,
                on=id_col,
                how="left_anti"
            ).withColumn("effective_date", current_date()).withColumn("is_current", lit(True))

            # Write updates, new records, and non-matching records
            if not updates.isEmpty():
                updates.write.mode("append").insertInto(table_name)
            if not new_records.isEmpty():
                new_records.write.mode("append").insertInto(table_name)
            if not non_matching.isEmpty():
                non_matching.write.mode("append").insertInto(table_name)

            print(f"Updated records in {table_name} table.")

        except Exception as e:
            self.load_success = False
            print(f"Failed to process SCD Type 2 for {table_name}: {e}")

    def load_dim_customer(self, df_dim_customer):
        """Load dim_customer incrementally and handle SCD Type 2 logic."""
        self._process_scd_type2(
            df_dim_customer,
            table_name="dim_customer",
            dimension_cols=["first_name", "last_name", "email", "phone_number"],
            id_col="customer_id",
            effective_col="effective_date",
            is_current_col="is_current"
        )

    def load_dim_product(self, df_dim_product):
        """Load dim_product incrementally and handle SCD Type 2 logic."""
        self._process_scd_type2(
            df_dim_product,
            table_name="dim_product",
            dimension_cols=["product_name", "category", "price"],
            id_col="product_id",
            effective_col="effective_date",
            is_current_col="is_current"
        )

    def load_dim_employee(self, df_dim_employee):
        """Load dim_employee data directly into Hive without SCD Type 2 logic."""
        try:
            table_name = "dim_employee"
            df_dim_employee.write.mode("append").insertInto(table_name)
            print(f"Employee data loaded into {table_name} table.")
        except Exception as e:
            self.load_success = False
            print(f"Failed to load employee data into {table_name} table: {e}")

    def load_all_dataframes(self, df_fact_sales, df_dim_customer, df_dim_product, df_dim_employee):
        """Load all dataframes (fact and dimension tables) into Hive."""
        try:
            # Load fact sales
            self.load_fact_sales(df_fact_sales)

            # Load dimension tables
            self.load_dim_customer(df_dim_customer)
            self.load_dim_product(df_dim_product)
            self.load_dim_employee(df_dim_employee)

        except Exception as e:
            self.load_success = False
            print(f"Failed to load all dataframes: {e}")

    def all_data_loaded(self):
        """Return the success flag indicating if all dataframes were loaded successfully."""
        return self.load_success
