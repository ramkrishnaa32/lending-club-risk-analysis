import getpass
from pyspark.sql import SparkSession
import lib.constants as constants

def initialize_spark_session(AppName):
    """
    Initializes and returns a Spark session with predefined configurations.

    Parameters:
        AppName (str): The name of the Spark application.

    Returns:
        SparkSession: A configured Spark session.
    """
    username = getpass.getuser()
    spark = SparkSession. \
    builder. \
    appName(AppName). \
    config('spark.ui.port', '0'). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", f'/Users/{username}/warehouse'). \
    enableHiveSupport(). \
    master('local'). \
    getOrCreate()
    return spark