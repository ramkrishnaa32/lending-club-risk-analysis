import time
import constants, helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

raw_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(f"{constants.path}/accepted_2007_to_2018Q4.csv")

print("Loaded data from CSV")

# print(raw_df.rdd.getNumPartitions())
# raw_df.createOrReplaceTempView("lending_club_data")
# spark.sql("select * from lending_club_data")

# since member_id null acrosss all rows, we can create the memeber_id based on few columns refering to the data dictionary
# ["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]
from pyspark.sql.functions import sha2, concat_ws
new_df = raw_df.withColumn("name_sha2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))
# new_df.select("name_sha2").show(truncate=False)
# print(new_df.select("name_sha2").distinct().count())

new_df.createOrReplaceTempView("newtable")
query = """
select name_sha2, count(*) as total_cnt
from newtable
group by name_sha2
having total_cnt>1
order by total_cnt desc
"""
# spark.sql(query).show(truncate=False)

# Validating the data against the sha2 value of "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" 
# since it has 33 entires, we can validate the data against this value
# all the columns are null for this value
# spark.sql("select * from newtable where name_sha2 like 'e3b0c44298fc1c149%'").show(10, truncate=False)

# Seperating the customer data and storing it
print("seperating the customer data and storing it")
customer_data_query = """
select name_sha2 as member_id, emp_title, emp_length, home_ownership, annual_inc, addr_state, zip_code, 'USA' as country, 
grade, sub_grade, verification_status, tot_hi_cred_lim, application_type, annual_inc_joint, verification_status_joint
from newtable
"""
spark.sql(customer_data_query).repartition(1).write \
.option("header", "true") \
.format("csv") \
.mode("overwrite") \
.option("path", f"{constants.path}/customers_data") \
.save()

# seperating the loan data and storing it
print("seperating the loan data and storing it")
loan_data_query = """
select id as loan_id, name_sha2 as member_id, loan_amnt, funded_amnt, term,int_rate, 
installment, issue_d, loan_status, purpose, title 
from newtable
"""
spark.sql(loan_data_query).repartition(1).write \
.option("header",True) \
.format("csv") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_data") \
.save()

# seperating the loan repayment data and storing it
print("seperating the loan repayment data and storing it")
repayment_data_query = """
select id as loan_id, total_rec_prncp, total_rec_int, total_rec_late_fee, 
total_pymnt, last_pymnt_amnt, last_pymnt_d, next_pymnt_d
from newtable
"""
spark.sql(repayment_data_query).repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_repayments") \
.save()

# seperating the loan defaulters data and storing it
print("seperating the loan defaulters data and storing it")
loan_defualters_query = """
select name_sha2 as member_id, delinq_2yrs, delinq_amnt, pub_rec, pub_rec_bankruptcies, 
inq_last_6mths, total_rec_late_fee, mths_since_last_delinq, mths_since_last_record
from newtable
"""
spark.sql(loan_defualters_query).repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", f"{constants.path}/loans_defaulters") \
.save()

print("completed data seperation")

# # Keep the session active to access the UI
# print("Access Spark UI at http://localhost:4040")
# time.sleep(300)  # Keep the session active for 5 minutes  