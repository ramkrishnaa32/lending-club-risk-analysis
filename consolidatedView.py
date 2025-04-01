"""
1. Creating a consolidated view and table
2. It should be update the view with the latest data with 24 hours
3. Checking the data in the view
4. The table should be refreshed with the latest data in every 7 days
5. Checking the data in the table
"""
import lib.constants as constants, lib.helperFunctions as helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("SHOW DATABASES").show()
spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

spark.sql("""
CREATE or REPLACE VIEW lending_club.customers_loan_v AS SELECT
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.join_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths

FROM lending_club.customers c
LEFT JOIN lending_club.loans l on c.member_id = l.member_id
LEFT JOIN lending_club.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN lending_club.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN lending_club.loans_defaulters_details e ON c.member_id = e.member_id
""")

spark.sql("select * from lending_club.customers_loan_v").show(10, truncate=False)

spark.sql("""
CREATE TABLE lending_club.customers_loan_t AS SELECT
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.join_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths

FROM lending_club.customers c
LEFT JOIN lending_club.loans l on c.member_id = l.member_id
LEFT JOIN lending_club.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN lending_club.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN lending_club.loans_defaulters_details e ON c.member_id = e.member_id
""")

spark.sql("select * from lending_club.customers_loan_t").show(10, truncate=False)

spark.sql("SHOW TABLES").show(truncate=False)
