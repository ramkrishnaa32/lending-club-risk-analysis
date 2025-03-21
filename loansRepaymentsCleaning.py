import constants, helperFunctions
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace, col, when, length, count

"""
1. Creating dataframe with proper schema
2. inserting new column with ingestion date, current date
3. Remove the duplicate rows
4. Dropping the rows which has null values in the mentioned columns
5. total_principal_received is null, then total_payment_received is 0.0 and total_principal_received != 0.0, then total_payment_received = total_principal_received + total_interest_received + total_late_fee_received
6. last_payment_date and next_payment_date is 0.0, then replace with None
7. write the cleaned data to the disk
"""

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

# Creating dataframe with proper schema
schema = 'loan_id string, total_principal_received float, total_interest_received float, \
         total_late_fee_received float, total_payment_received float, last_payment_amount float, \
         last_payment_date string, next_payment_date string'

loans_repayments = spark.read \
.format("csv") \
.option("header","true") \
.schema(schema) \
.load(f"{constants.path}/loans_repayments")

# Adding ingestion date
loans_repayments = loans_repayments.withColumn("ingest_date", current_timestamp())

loans_repayments.createOrReplaceTempView("loans_repayments")
# spark.sql("select * from loans_repayments").show(truncate=False)
# print(spark.sql("select count(*) from loans_repayments where total_principal_received is null").count())

# Checling the null values in the columns
columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]
loans_repayments = loans_repayments.dropna(subset = columns_to_check)
loans_repayments.createOrReplaceTempView("loans_repayments")
# print(spark.sql("select * from loans_repayments where total_payment_received = 0.0 and total_principal_received != 0.0 ").count())

loans_repayments = loans_repayments.withColumn(
    "total_payment_received", 
    when(
        (col("total_principal_received") != 0.0) & 
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
) 

loans_repayments = loans_repayments.withColumn(
  "last_payment_date",
   when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
)

loans_repayments = loans_repayments.withColumn(
  "next_payment_date",
   when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
)

# Storing the cleaned data
loans_repayments.write \
.format("parquet") \
.mode("overwrite") \
.save(f"{constants.path}/loans_repayments_cleaned")


