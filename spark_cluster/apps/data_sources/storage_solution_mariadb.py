TABLE_EMPLOYEE = "Employees"

def write_to_mysql(_df):
    """
    This method is to write data back to mysql
    """
    _df.write \
        .format("jdbc") \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .option("url", URL) \
        .option("dbtable", TABLE_EMPLOYEE) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .save()

def read_from_mysql(_spark):
    """
    This method is going to read data from mysql database
    """
    return _spark.read.format("jdbc") \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .option("url", URL) \
        .option("dbtable", TABLE_EMPLOYEE) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .load()