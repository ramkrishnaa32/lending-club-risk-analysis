import constants
import helperFunctions

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

helperFunctions.send_email(constants.receiver_email, subject, body)