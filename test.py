from pyspark.sql import SparkSession
import getpass
import time
username = getpass.getuser()

spark = SparkSession. \
builder. \
config('spark.ui.port', '0'). \
config('spark.shuffle.useOldFetchProtocol', 'true'). \
config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
enableHiveSupport(). \
master('local'). \
getOrCreate()

print("Initiated Spark session with Hive support")
# print(f"Spark version: {spark.version}") 

path = "/Users/kramkrishnaachary/Learning/Trendytech/lending_club_project/data/accepted_2007_to_2018Q4.csv"
raw_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(path)

print("Loaded data from CSV")

raw_df.printSchema()
raw_df.show(5, truncate=False)

# # Keep the session active to access the UI
# print("Access Spark UI at http://localhost:4040")
# time.sleep(300)  # Keep the session active for 5 minutes