import constants, helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

# rm -rf /Users/kramkrishnaachary/warehouse/lending_club.db
# Creating database with name lending_club
spark.sql("CREATE DATABASE IF NOT EXISTS lending_club")
spark.sql("SHOW DATABASES").show()
spark.sql("DESCRIBE DATABASE EXTENDED lending_club").show(truncate=False)
spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

# Creating external table for clenaned customers data
spark.sql("DROP TABLE IF EXISTS lending_club.customers")
spark.sql("""create external table IF NOT EXISTS lending_club.customers (
             member_id string, emp_title string, emp_length int, home_ownership string, annual_income float, 
             address_state string, address_zipcode string, address_country string, grade string, sub_grade string, 
             verification_status string, total_high_credit_limit float, application_type string, join_annual_income float, 
             verification_status_joint string, ingest_date timestamp)
             stored as parquet 
             location '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/customers_data_cleaned'""")

# spark.sql("DESCRIBE extended lending_club.customers").show(50, truncate=False)
spark.sql("select * from lending_club.customers").show(10, truncate=False)

# Creating external table for cleaned loans data
spark.sql("DROP TABLE IF EXISTS lending_club.loans")
spark.sql("""create external table IF NOT EXISTS lending_club.loans (
             loan_id string, member_id string, loan_amount float, funded_amount float,
             loan_term_years integer, interest_rate float, monthly_installment float, issue_date string,
             loan_status string, loan_purpose string, loan_title string, ingest_date timestamp)
             stored as parquet
             location '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/loans_data_cleaned'""")

# spark.sql("DESCRIBE extended lending_club.loans").show(50, truncate=False)
spark.sql("select * from lending_club.loans").show(10, truncate=False)

# Creating external table for cleaned loans repayments data
spark.sql("DROP TABLE IF EXISTS lending_club.loans")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loans_repayments (
             loan_id string, total_principal_received float, total_interest_received float, 
             total_late_fee_received float, total_payment_received float, last_payment_amount float,
             last_payment_date string, next_payment_date string, ingest_date timestamp)
             stored as parquet 
             LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/loans_repayments_cleaned'""")

# spark.sql("DESCRIBE extended lending_club.loans_repayments").show(50, truncate=False)
spark.sql("select * from lending_club.loans_repayments").show(10, truncate=False)

# Creating external table for cleaned loans defaulters data
spark.sql("DROP TABLE IF EXISTS lending_club.loans_defaulters_delinq")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loans_defaulters_delinq (
             member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
             stored as parquet 
             LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/loans_defaulters_delinq'""")

spark.sql("select * from lending_club.loans_defaulters_delinq").show(10, truncate=False)

spark.sql("DROP TABLE IF EXISTS lending_club.loans_defaulters_details")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS lending_club.loans_defaulters_details (
             member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
             stored as parquet 
             LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/loans_defaulters_enq_details'""")

spark.sql("select * from lending_club.loans_defaulters_details").show(10, truncate=False)
