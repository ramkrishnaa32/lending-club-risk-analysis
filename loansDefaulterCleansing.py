import constants, helperFunctions
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace, col, when, length

"""
1. Creating dataframe with proper schema
2. Columns name are not proper, so renaming the columns
3. Inseting new column with ingestion date, current date
4. delinq_2yrs to int
5. Remove the duplicate rows

"""

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

schema = 'member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float, \
         inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float'

loans_defaulters = spark.read \
.format("csv") \
.option("header","true") \
.schema(schema) \
.load(f"{constants.path}/loans_defaulters")

loans_defaulters.printSchema()

# converting to int for few columns and fill na with 0
loans_defaulters = loans_defaulters.withColumn("delinq_2yrs", col("delinq_2yrs").cast("int")) \
                                    .fillna(0, subset=["delinq_2yrs"]) \
                                    .withColumn("pub_rec", col("pub_rec").cast("int")) \
                                    .fillna(0, subset=["pub_rec"]) \
                                    .withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("int")) \
                                    .fillna(0, subset=["pub_rec_bankruptcies"]) \
                                    .withColumn("inq_last_6mths", col("inq_last_6mths").cast("int")) \
                                    .fillna(0, subset=["inq_last_6mths"])

# loans_defaulters.createOrReplaceTempView("loans_defaulters")

# spark.sql("""select delinq_2yrs, count(*) as totalCount 
#             from loans_defaulters
#             group by delinq_2yrs 
#             order by totalCount desc""").show(truncate=False)

# adding ingestion date
loans_defaulters = loans_defaulters.withColumn("ingest_date", current_timestamp())

loans_defaulters.createOrReplaceTempView("loans_defaulters")

query = """select member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq), ingest_date
            from loans_defaulters 
            where delinq_2yrs > 0 or mths_since_last_delinq > 0"""

loans_def_delinq = spark.sql(query)
print(loans_def_delinq.count())

public_record_query = """select member_id
                      from loans_defaulters 
                      where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0"""

loans_def_records_enq = spark.sql(public_record_query)
print(loans_def_records_enq.count())

public_record_query_details = """select member_id, pub_rec, pub_rec_bankruptcies, inq_last_6mths, ingest_date
                                 from loans_defaulters"""

pub_records_enq_details = spark.sql(public_record_query_details)
print(loans_def_records_enq.count())

loans_def_delinq.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_defaulters_delinq") \
.save()

loans_def_records_enq.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_defaulters_records_enq") \
.save()

pub_records_enq_details.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_defaulters_enq_details") \
.save()

print("Data stored successfully")