import constants
import helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

# Email content for bad data cleansing notification
subject = "Lending Club Risk Analysis - Bad Data Cleansing"
body = """
Hello Team,

I hope you're doing well.

This email serves as a notification regarding the bad data cleansing process for the Lending Club risk analysis project.  
Please ensure that all data anomalies and inconsistencies are properly addressed to maintain the integrity of our analysis.  

Let me know if you need any further details.

Regards,
K Ramkrishna Achary
"""
attachment_path = f"{constants.path}/duplicate_member_ids/part-00000-aa326d4a-264e-4ba2-b085-5d2adf446882-c000.csv"

helperFunctions.send_email(constants.receiver_email, subject, body, attachment_path)