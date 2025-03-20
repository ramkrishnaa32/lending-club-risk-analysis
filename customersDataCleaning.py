import constants, helperFunctions
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace, col, when, length

"""
1. Creating dataframe with proper schema
2. Columns name are not proper, so renaming the columns
3. Inseting new column with ingestion date, current date
4. Remove the dupicate rows
5. Remove the rows with annuall income as null
6. Convert emp_length to numeric, replace null with average value
7. Clean the address_state with two letter code, if not present then repalce with NA
"""

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

schema = """member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, 
            zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, 
            application_type string, annual_inc_joint float, verification_status_joint string"""

customer_data = spark.read \
.format("csv") \
.option("header","true") \
.schema(schema) \
.load(f"{constants.path}/customers_data")

# Changing the column names
customer_data = customer_data.withColumnRenamed("annual_inc", "annual_income") \
                             .withColumnRenamed("addr_state", "address_state") \
                             .withColumnRenamed("zip_code", "address_zipcode") \
                             .withColumnRenamed("country", "address_country") \
                             .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
                             .withColumnRenamed("annual_inc_joint", "join_annual_income")
                             
# Adding ingestion date
customer_data = customer_data.withColumn("ingest_date", current_timestamp())

# Removing the duplicate rows
customer_distinct = customer_data.dropDuplicates()

# creating temp view
customer_distinct.createOrReplaceTempView("customers")

# Removing the rows with annual income as null
customer_income_filter = spark.sql("select * from customers where annual_income is not null")
customer_income_filter.createOrReplaceTempView("customers")

# Converting emp_length to numeric
# spark.sql("select emp_length from customers limit 20").show(10, truncate=False)
customers_emp_length_casted = customer_income_filter.withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)", "")) \
                                                    .withColumn("emp_length", col("emp_length").cast("int"))

customers_emp_length_casted.createOrReplaceTempView("customers")
query = """select floor(avg(emp_length)) as avg_emp_length from customers"""
avg_emp_length = spark.sql(query).collect()[0][0]
customers_emp_length_replace = customers_emp_length_casted.fillna(avg_emp_length, subset=["emp_length"])

# Cleaning the address_state
customers_emp_length_replace.createOrReplaceTempView("customers")
query = """select count(address_state) from customers where length(address_state) > 2"""
# spark.sql(query).show(truncate=False)

customers_state_cleaned = customers_emp_length_replace \
                                .withColumn("address_state", when(length(col("address_state")) > 2, "NA").otherwise(col("address_state")))

customers_state_cleaned.createOrReplaceTempView("customers")

# Storing the cleaned data
customers_state_cleaned.write \
.format("parquet") \
.mode("overwrite") \
.save(f"{constants.path}/customers_data_cleaned")

# spark.sql("select * from customers").show(10, truncate=False)
# spark.sql("select * from customers where annual_income is null").show(10, truncate=False)
# spark.sql("select * from customers where emp_length is null").show(10, truncate=False)
# spark.sql("select * from customers where address_state is null").show(10, truncate=False)
# spark.sql("select * from customers where address_state like 'NA'").show(10, truncate=False)
# spark.sql("select * from customers where address_state not like 'NA'").show(10, truncate=False)
# spark.sql("select * from customers where address_state not like 'NA'").show(10, truncate=False) 
# spark.sql("select * from customers where emp_length is null").show(10, truncate=False)

print("Data cleaning completed")
spark.stop()
