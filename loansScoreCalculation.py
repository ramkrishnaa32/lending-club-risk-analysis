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

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

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

