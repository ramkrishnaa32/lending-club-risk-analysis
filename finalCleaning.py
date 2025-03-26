"""
1. Cleaning the data and storing it in the disk
2. Remmving duplicate member ids from the customers, loans_defaulters_delinq and loans_defaulters_details tables
3. Storing the duplicate member ids for notifying the team
4. Combining all the member ids from the above tables
5. Storing the duplicate member ids
6. Filtering the duplicate member ids from the customers, loans_defaulters_delinq and loans_defaulters_details tables
7. Storing the filtered data
8. Creating external tables for the cleaned data
"""

# Importing the required libraries
import helperFunctions
import constants

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("USE lending_club")
# spark.sql("SHOW TABLES").show(truncate=False)

"""
+------------+------------------------+-----------+
|namespace   |tableName               |isTemporary|
+------------+------------------------+-----------+
|lending_club|customers               |false      |
|lending_club|customers_loan_t        |false      |
|lending_club|customers_loan_v        |false      |
|lending_club|loans                   |false      |
|lending_club|loans_defaulters_delinq |false      |
|lending_club|loans_defaulters_details|false      |
|lending_club|loans_repayments        |false      |
+------------+------------------------+-----------+"
"""

# spark.sql("""
#           SELECT member_id, count(*) as totalCount
#           FROM lending_club.customers
#           group by member_id
#           order by totalCount desc""").show(10, truncate=False)

# spark.sql(""" SELECT * 
#               FROM lending_club.customers 
#               where member_id = 'e4c167053d54182305bf5ea081111e06bf298584cfcaebe792744eee19461f7f'""").show(10, truncate=False)

# spark.sql("""SELECT member_id, count(*) as totalCount
#              FROM lending_club.loans_defaulters_delinq
#              group by member_id
#              order by totalCount desc""").show(10, truncate=False)

# spark.sql(""" SELECT * 
#               FROM lending_club.loans_defaulters_delinq 
#               where member_id = 'e4c167053d54182305bf5ea081111e06bf298584cfcaebe792744eee19461f7f'""").show(10, truncate=False)

# spark.sql("""SELECT member_id, count(*) as totalCount
#              FROM lending_club.loans_defaulters_details
#              group by member_id
#              order by totalCount desc""").show(10, truncate=False)

# spark.sql(""" SELECT * 
#               FROM lending_club.loans_defaulters_details 
#               where member_id = 'e4c167053d54182305bf5ea081111e06bf298584cfcaebe792744eee19461f7f'""").show(10, truncate=False)

# Filtering the duplicate customer ids in customers, loans_defaulters_delinq and loans_defaulters_details tables
customer_ids_duplicates = spark.sql(""" SELECT member_id FROM (
          SELECT member_id, count(*) as totalCount
          FROM lending_club.customers
          group by member_id
          having totalCount > 1)""")

defaulters_delinq_duplicates = spark.sql(""" SELECT member_id FROM (
             SELECT member_id, count(*) as totalCount
             FROM lending_club.loans_defaulters_delinq
             group by member_id
             having totalCount > 1)""")

defaulters_details_duplicates = spark.sql(""" SELECT member_id FROM (
             SELECT member_id, count(*) as totalCount
             FROM lending_club.loans_defaulters_details
             group by member_id
             having totalCount > 1)""")

print("Number of duplicate customer ids in customers table: ", customer_ids_duplicates.count())
print("Number of duplicate customer ids in loans_defaulters_delinq table: ", defaulters_delinq_duplicates.count())
print("Number of duplicate customer ids in loans_defaulters_details table: ", defaulters_details_duplicates.count())

# Storing the duplicate customer ids for notifiying the team
customer_ids_duplicates.repartition(1).write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.option("path", f"{constants.path}/customers_duplicates") \
.save()

defaulters_delinq_duplicates.repartition(1).write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.option("path", f"{constants.path}/loans_defaulters_delinq_duplicates") \
.save()

defaulters_details_duplicates.repartition(1).write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.option("path", f"{constants.path}/loans_defaulters_details_duplicates") \
.save()

# Combining all the member ids from the above tables
duplicate_member_ids = customer_ids_duplicates.union(defaulters_delinq_duplicates).union(defaulters_details_duplicates).distinct()
print("Total number of duplicate member ids: ", duplicate_member_ids.count())

# Storing the duplicate member ids
duplicate_member_ids.repartition(1).write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.option("path", f"{constants.path}/duplicate_member_ids") \
.save()

duplicate_member_ids.createOrReplaceTempView("duplicate_member_ids")

# Filtering the duplicate member ids from the customers, loans_defaulters_delinq and loans_defaulters_details tables
customers_filtered = spark.sql(""" SELECT * FROM lending_club.customers
          where member_id not in (select member_id from duplicate_member_ids)""")

loans_defaulters_delinq_filtered = spark.sql(""" SELECT * FROM lending_club.loans_defaulters_delinq
             where member_id not in (select member_id from duplicate_member_ids)""")

loans_defaulters_details_filtered = spark.sql(""" SELECT * FROM lending_club.loans_defaulters_details 
                                              where member_id not in (select member_id from duplicate_member_ids)""")

# Storing the filtered data
customers_filtered.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/cleaned/customers") \
.save()

loans_defaulters_delinq_filtered.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/cleaned/loans_defaulters_delinq") \
.save()

loans_defaulters_details_filtered.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/cleaned/loans_defaulters_details") \
.save()

# Creating external tables for the cleaned data
spark.sql("""DROP TABLE IF EXISTS lending_club.customers_c""")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.customers_c (member_id string, emp_title string, emp_length int, 
home_ownership string, annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
sub_grade string, verification_status string, total_high_credit_limit float, application_type string, 
join_annual_income float, verification_status_joint string, ingest_date timestamp)
stored as parquet
LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/cleaned/customers'""")

spark.sql("""DROP TABLE IF EXISTS lending_club.loans_defaulters_delinq_c""")
spark.sql("""
CREATE EXTERNAL TABLE lending_club.loans_defaulters_delinq_c(member_id string,delinq_2yrs integer, delinq_amnt float, 
mths_since_last_delinq integer)
stored as parquet
LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/cleaned/loans_defaulters_delinq'""")

spark.sql("""DROP TABLE IF EXISTS lending_club.loans_defaulters_details_c""")
spark.sql("""
CREATE EXTERNAL TABLE lending_club.loans_defaulters_details_c(member_id string, pub_rec integer, pub_rec_bankruptcies integer, 
inq_last_6mths integer)
stored as parquet
LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/cleaned/loans_defaulters_details'""")

spark.sql("SHOW TABLES").show(truncate=False)

spark.sql(""" SELECT member_id, count(*) as totalCount
              from lending_club.customers_c
              group by member_id order by totalCount desc""").show(10, truncate=False)

print("Completed, Final cleaned data stored successfully")
