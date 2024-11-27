import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main(sql_server_details):
    spark = (
        SparkSession.builder.appName("Parquet-to-SQLServer")
        .config("spark.ui.port", "4040")
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8")
        .getOrCreate()
    )

    # Read Parquet files for each topic
    tweets_df = spark.read.parquet("/opt/spark-data/final_project/tweets_output")
    reposts_df = spark.read.parquet("/opt/spark-data/final_project/reposts_output")
    likes_df = spark.read.parquet("/opt/spark-data/final_project/likes_output")
    comments_df = spark.read.parquet("/opt/spark-data/final_project/comments_output")

    # Tweets that contain the word "president"
    filtered_tweets_df = tweets_df.filter(col("content").contains("president"))
    # Reposts of the first post
    filtered_reposts_df = reposts_df.filter(col("original_post_id") == 1)
    # Likes of the second post
    filtered_likes_df = likes_df.filter(col("post_id") == 2)
    # Comments made by first user
    filtered_comments_df = comments_df.filter(col("user_id") == 1)

    # SQL Server connection settings
    database_url = f"jdbc:sqlserver://{sql_server_details['host']};databaseName={sql_server_details['database']}"
    connection_properties = {
        "user": sql_server_details["user"],
        "password": sql_server_details["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    # Writing the filtered DataFrames to SQL Server
    filtered_tweets_df.write.jdbc(
        url=database_url,
        table="tweets",
        mode="append",
        properties=connection_properties,
    )
    filtered_reposts_df.write.jdbc(
        url=database_url,
        table="reposts",
        mode="append",
        properties=connection_properties,
    )
    filtered_likes_df.write.jdbc(
        url=database_url, table="likes", mode="append", properties=connection_properties
    )
    filtered_comments_df.write.jdbc(
        url=database_url,
        table="comments",
        mode="append",
        properties=connection_properties,
    )

    spark.stop()


if __name__ == "__main__":
    sql_server_config = {
        "host": "netflixs-dataframe.database.windows.net:1433",
        "database": "twitter_analytics",
        "user": "bigdata",
        "password": "Bigdaddy!",
    }

    main(sql_server_config)
