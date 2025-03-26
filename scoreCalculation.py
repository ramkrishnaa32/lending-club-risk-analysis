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

import constants
import helperFunctions
from pyspark.sql.functions import col

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

# Associating points to the grades in order to calculate the Loan Score
# User defined variables
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)

spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)

"""
The tables required to calculate the Loan Score
customers_c
loans
loans_repayments
loans_defaulters_delinq_c
loans_defaulters_detail_c
"""

# Loading duplicate customer ids
duplicate_member_ids = spark.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.load(f"{constants.path}/duplicate_member_ids")

duplicate_member_ids.createOrReplaceTempView("duplicate_member_ids")

# Loan Score Calculation Criteria 1: Payment History (ph)
payment_history = spark.sql("select c.member_id, \
   case \
   when p.last_payment_amount < (c.monthly_installment * 0.5) then ${spark.sql.very_bad_rated_pts} \
   when p.last_payment_amount >= (c.monthly_installment * 0.5) and p.last_payment_amount < c.monthly_installment then ${spark.sql.very_bad_rated_pts} \
   when (p.last_payment_amount = (c.monthly_installment)) then ${spark.sql.good_rated_pts} \
   when p.last_payment_amount > (c.monthly_installment) and p.last_payment_amount <= (c.monthly_installment * 1.50) then ${spark.sql.very_good_rated_pts} \
   when p.last_payment_amount > (c.monthly_installment * 1.50) then ${spark.sql.excellent_rated_pts} \
   else ${spark.sql.unacceptable_rated_pts} \
   end as last_payment_pts, \
   case \
   when p.total_payment_received >= (c.funded_amount * 0.50) then ${spark.sql.very_good_rated_pts} \
   when p.total_payment_received < (c.funded_amount * 0.50) and p.total_payment_received > 0 then ${spark.sql.good_rated_pts} \
   when p.total_payment_received = 0 or (p.total_payment_received) is null then ${spark.sql.unacceptable_rated_pts} \
   end as total_payment_pts \
   from lending_club.loans_repayments p \
   INNER JOIN \
   lending_club.loans c on c.loan_id = p.loan_id \
   WHERE member_id NOT IN (select member_id from duplicate_member_ids)")

payment_history.createOrReplaceTempView("payment_history")

# Loan Score Calculation Criteria 2: Loan Defaulters History (ldh)
loan_defaulter_history = spark.sql(
    "SELECT p.*, \
    CASE \
    WHEN d.delinq_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN ${spark.sql.unacceptable_grade_pts} \
    END AS delinq_pts, \
    CASE \
    WHEN l.pub_rec = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.pub_rec BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.pub_rec BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.pub_rec > 5 OR l.pub_rec IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END AS public_records_pts, \
    CASE \
    WHEN l.pub_rec_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END as public_bankruptcies_pts, \
    CASE \
    WHEN l.inq_last_6mths = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.inq_last_6mths BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.inq_last_6mths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS enq_pts \
    FROM lending_club.loans_defaulters_details_c l \
    INNER JOIN \
    lending_club.loans_defaulters_delinq_c d ON d.member_id = l.member_id  \
    INNER JOIN \
    payment_history p ON p.member_id = l.member_id \
    WHERE l.member_id NOT IN (select member_id from duplicate_member_ids)")

loan_defaulter_history.createOrReplaceTempView("loan_defaulter_payment_history")

# Loan Score Calculation Criteria 3: Financial Health (fh)
fh_ldh_ph_df = spark.sql(
   "SELECT ldef.*, \
   CASE \
   WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${spark.sql.excellent_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${spark.sql.good_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${spark.sql.bad_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.very_bad_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${spark.sql.unacceptable_rated_pts} \
   else ${spark.sql.unacceptable_rated_pts} \
   END AS loan_status_pts, \
   CASE \
   WHEN LOWER(a.home_ownership) LIKE '%own' THEN ${spark.sql.excellent_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%rent' THEN ${spark.sql.good_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN ${spark.sql.bad_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts} \
   END AS home_pts, \
   CASE \
   WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN ${spark.sql.very_good_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN ${spark.sql.good_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN ${spark.sql.bad_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN ${spark.sql.very_bad_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts} \
   else ${spark.sql.unacceptable_rated_pts} \
   END AS credit_limit_pts, \
   CASE \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A1' THEN ${spark.sql.excellent_rated_pts} \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A2' THEN (${spark.sql.excellent_rated_pts} * 0.95) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A3' THEN (${spark.sql.excellent_rated_pts} * 0.90) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A4' THEN (${spark.sql.excellent_rated_pts} * 0.85) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A5' THEN (${spark.sql.excellent_rated_pts} * 0.80) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B1' THEN (${spark.sql.very_good_rated_pts}) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B2' THEN (${spark.sql.very_good_rated_pts} * 0.95) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B3' THEN (${spark.sql.very_good_rated_pts} * 0.90) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B4' THEN (${spark.sql.very_good_rated_pts} * 0.85) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B5' THEN (${spark.sql.very_good_rated_pts} * 0.80) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C1' THEN (${spark.sql.good_rated_pts}) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C2' THEN (${spark.sql.good_rated_pts} * 0.95) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C3' THEN (${spark.sql.good_rated_pts} * 0.90) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C4' THEN (${spark.sql.good_rated_pts} * 0.85) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C5' THEN (${spark.sql.good_rated_pts} * 0.80) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D1' THEN (${spark.sql.bad_rated_pts}) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D2' THEN (${spark.sql.bad_rated_pts} * 0.95) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D3' THEN (${spark.sql.bad_rated_pts} * 0.90) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D4' THEN (${spark.sql.bad_rated_pts} * 0.85) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D5' THEN (${spark.sql.bad_rated_pts} * 0.80) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E1' THEN (${spark.sql.very_bad_rated_pts}) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E2' THEN (${spark.sql.very_bad_rated_pts} * 0.95) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E3' THEN (${spark.sql.very_bad_rated_pts} * 0.90) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E4' THEN (${spark.sql.very_bad_rated_pts} * 0.85) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E5' THEN (${spark.sql.very_bad_rated_pts} * 0.80) \
   WHEN (a.grade) in ('F', 'G') THEN (${spark.sql.unacceptable_rated_pts}) \
   END AS grade_pts \
   FROM loan_defaulter_payment_history ldef \
   INNER JOIN lending_club.loans l ON ldef.member_id = l.member_id \
   INNER JOIN lending_club.customers_c a ON a.member_id = ldef.member_id \
   WHERE ldef.member_id NOT IN (SELECT member_id FROM duplicate_member_ids)") 

fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

"""
Final loan score calculation by considering all the 3 criterias with the following percentage
1. Payment History = 20%
2. Loan Defaults = 45%
3. Financial Health = 35%
"""

loan_score = spark.sql(
    "SELECT member_id, \
    ((last_payment_pts + total_payment_pts) * 0.20) as payment_history_pts, \
    ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, \
    ((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) as financial_health_pts \
    FROM fh_ldh_ph_pts")

final_loan_score = loan_score.withColumn('loan_score', loan_score.payment_history_pts + loan_score.defaulters_history_pts + loan_score.financial_health_pts)

final_loan_score.createOrReplaceTempView("loan_score_eval")

loan_score_final = spark.sql(
    "SELECT ls.*, \
    CASE \
    WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN 'A' \
    WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN 'B' \
    WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN 'C' \
    WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score  > ${spark.sql.very_bad_grade_pts} THEN 'D' \
    WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN 'E'  \
    WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN 'F' \
    END as loan_final_grade \
    FROM loan_score_eval ls")

decimal_cols = ["payment_history_pts", "defaulters_history_pts", "financial_health_pts", "loan_score"]
for column in decimal_cols:
    loan_score_final = loan_score_final.withColumn(column, col(column).cast("float"))

loan_score_final.write \
.format("parquet") \
.mode("overwrite") \
.option("path", f"{constants.path}/final_score") \
.save()

print(loan_score_final.printSchema())

# Creating external table
spark.sql("""DROP TABLE IF EXISTS lending_club.score""")
spark.sql("""
CREATE EXTERNAL TABLE lending_club.score (member_id string, payment_history_pts float, defaulters_history_pts float, 
financial_health_pts float, loan_score float, loan_final_grade string)
stored as parquet
LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/lending_club_project/data/final_score'""")