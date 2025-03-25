"""
Loan Score Calculation:
Higher the score higher chance of getting loan approved
    1. Loan repayment history (last payment, total payment received, loan status, funded amount, grade)
    2. Loan defaulters history (delinquency, public records, inquiries)
    3. Customer finacial health (annual income, home ownership)

Wetghtage:
    repayment history - 20%
    defaulters history - 45%
    financial health - 35%
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


