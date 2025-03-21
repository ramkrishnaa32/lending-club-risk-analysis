import getpass
from pyspark.sql import SparkSession

def initialize_spark_session(AppName):
    username = getpass.getuser()
    spark = SparkSession. \
    builder. \
    appName(AppName). \
    config('spark.ui.port', '0'). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('local'). \
    getOrCreate()
    return spark


