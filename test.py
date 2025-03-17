from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder.appName("Spark Test").getOrCreate()

print("Spark session created", spark)

# Create DataFrame
data = [("John", "Doe", 28), ("Jane", "Doe", 25), ("John", "Smith", 30)]
columns = ["first_name", "last_name", "age"]
df = spark.createDataFrame(data, columns)
df.show()

# Keep the session active to access the UI
print("Access Spark UI at http://localhost:4040")
time.sleep(300)  # Keep the session active for 5 minutes