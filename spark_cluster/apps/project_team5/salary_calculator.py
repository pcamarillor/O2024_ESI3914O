from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, current_timestamp, when, max as spark_max, monotonically_increasing_id

def get_last_transaction_id(db_url, db_table, db_properties):
    """
    Get the last transaction_id from the PostgreSQL table.
    Args:
        db_url (str): JDBC URL for the PostgreSQL database.
        db_table (str): Table name to query.
        db_properties (dict): Connection properties including user and password.
    Returns:
        int: The last transaction_id in the table, or 0 if the table is empty.
    """
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("GetLastTransactionID") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()

        # Read data from the table
        existing_df = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Get the maximum transaction_id
        last_transaction_id = existing_df.agg({"transaction_id": "max"}).collect()[0][0]

        # Return 0 if the table is empty
        return last_transaction_id if last_transaction_id is not None else 0

    except Exception as e:
        print("Error retrieving last transaction_id:", e)
        return 0

def save_to_postgresql(spark, df, db_url, db_table, db_properties):
    """
    Save a Spark DataFrame to PostgreSQL.
    Args:
        df (DataFrame): The Spark DataFrame to save.
        db_url (str): JDBC URL for the PostgreSQL database.
        db_table (str): Table name to append the data to.
        db_properties (dict): Connection properties including user and password.
    """
    try:
        # Read existing data from PostgreSQL
        existing_df = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Merge new data with existing data
        merged_df = existing_df.unionByName(df, allowMissingColumns=True).dropDuplicates(["employeeID", "payslip_timestamp"])

        # Save the merged data back to PostgreSQL
        merged_df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("Data successfully saved to PostgreSQL.")

    except Exception as e:
        print("Error saving to PostgreSQL:", e)

def calculate_salaries(datalake_path, employee_csv_path, db_url, db_table, db_properties):
    """
    Retrieve the latest exchange rates and calculate regional salaries for employees.
    Save the result to a PostgreSQL database.
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SalaryCalculator") \
        .config("spark.ui.port", "4040") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Loading exchange rates from data lake...")

    # Load the latest exchange rates from the data lake
    exchange_rates = spark.read.parquet(datalake_path)

    # Get the latest timestamped rates
    latest_rates = exchange_rates \
        .select("currency_pair", "max_rate", "window") \
        .filter(col("window").isNotNull()) \
        .groupBy("currency_pair") \
        .agg(spark_max("window").alias("latest_window"),
            spark_max("max_rate").alias("exchange_rate"))

    print("Loading employee salary data...")
    # Load employee data
    employee_df = spark.read.csv(employee_csv_path, header=True, inferSchema=True)

    # Join employee data with the latest exchange rates
    employee_with_rates = employee_df.join(
        latest_rates,
        employee_df["currency_region"] == latest_rates["currency_pair"],
        how="left"
    )

    employee_with_rates.show()

    # Add exchange rate and calculate regional salary
    payslip_df = employee_with_rates \
        .withColumn("exchange_rate", when(col("exchange_rate").isNull(),
                                          lit(1.0)).otherwise(col("exchange_rate"))) \
        .withColumn("regional_salary", round(col("usd_salary") * col("exchange_rate"), 2)) \
        .withColumn("payslip_timestamp", current_timestamp()) \
        .select(
            "employeeID", "usd_salary", "currency_region", "exchange_rate", "regional_salary", "payslip_timestamp"
        )

    # Get the last transaction_id
    last_transaction_id = get_last_transaction_id(db_url, db_table, db_properties)
    print(f"Last transaction_id retrieved: {last_transaction_id}")

    # Add the transaction_id column starting from the last transaction_id
    payslip_df = payslip_df.withColumn("transaction_id", monotonically_increasing_id() + lit(last_transaction_id + 1))

    # Reorder columns to place transaction_id at the start
    columns = ["transaction_id"] + [col for col in payslip_df.columns if col != "transaction_id"]
    payslip_df = payslip_df.select(*columns)

    payslip_df.show()

    # Save to PostgreSQL
    save_to_postgresql(spark, payslip_df, db_url, db_table, db_properties)

if __name__ == "__main__":
    # Define paths and database connection details
    DATALAKE_PATH = "/otp/spark-data/parquet/exchange_rates"
    EMPLOYEE_CSV_PATH = "/opt/spark-data/team5_project_data/employee_salary.csv"
    DB_URL = "jdbc:postgresql://host.docker.internal:5432/payslip_db"
    DB_TABLE = "payslips_transactions"
    DB_PROPERTIES = {"user": "user", "password": "password"}

    # Run salary calculation and save to database
    calculate_salaries(DATALAKE_PATH, EMPLOYEE_CSV_PATH, DB_URL, DB_TABLE, DB_PROPERTIES)
