import constants, helperFunctions
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace, col, when, length, count

"""
1. Creating dataframe with proper schema
2. inserting new column with ingestion date, current date
3. Remove the duplicate rows
4. Dropping the rows which has null values in the mentioned columns
5. converting the loan_term_months to years
6. Categorise the loan_purpose
"""

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

# Creating dataframe with proper schema
schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, \
          interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, \
          loan_title string'

loan_data = spark.read \
.format("csv") \
.option("header","true") \
.schema(schema) \
.load(f"{constants.path}/loans_data")

# Adding ingestion date
loan_data = loan_data.withColumn("ingest_date", current_timestamp())

loan_data.createOrReplaceTempView("loans")
print(spark.sql("select * from loans where loan_amount is null").count())

# Checling the null values in the columns
columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
loan_data = loan_data.dropna(subset = columns_to_check)
loan_data.createOrReplaceTempView("loans")

# converting the loan_term_months to years
loan_data = loan_data.withColumn("loan_term_months", regexp_replace(col("loan_term_months"), " months", "")) \
                     .withColumn("loan_term_months", col("loan_term_months").cast("int")/12) \
                     .withColumnRenamed("loan_term_months", "loan_term_years") \
                     .withColumn("loan_term_years", col("loan_term_years").cast("int"))

# loan_data.createOrReplaceTempView("loans")
# spark.sql("select loan_purpose, count(*) as total_count from loans group by loan_purpose order by total_count desc").show(10, truncate = False)

# Categorise the loan_purpose
loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", 
                       "major_purchase", "medical", "small_business", "car", "vacation", "moving", 
                       "house", "wedding", "renewable_energy", "educational"]

loan_cleaned_data = loan_data.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))

# loan_data.createOrReplaceTempView("loans")
# spark.sql("select loan_purpose, count(*) as total_count from loans group by loan_purpose order by total_count desc").show(truncate = False)

loan_data.groupBy("loan_purpose") \
    .agg(count("*").alias("total_count")) \
    .orderBy(col("total_count").desc()) \
    .show(truncate = False)

# Storing the cleaned data
loan_cleaned_data.write \
.format("parquet") \
.mode("overwrite") \
.save(f"{constants.path}/loans_data_cleaned")
