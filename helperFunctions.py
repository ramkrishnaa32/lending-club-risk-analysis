import getpass
from pyspark.sql import SparkSession
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import constants

def initialize_spark_session(AppName):
    """
    Initializes and returns a Spark session with predefined configurations.

    Parameters:
        AppName (str): The name of the Spark application.

    Returns:
        SparkSession: A configured Spark session.
    """
    username = getpass.getuser()
    spark = SparkSession. \
    builder. \
    appName(AppName). \
    config('spark.ui.port', '0'). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", f'/Users/{username}/warehouse'). \
    enableHiveSupport(). \
    master('local'). \
    getOrCreate()
    return spark

def send_email(to_email, subject, body):
    """
    Sends an email notification.

    Parameters:
        to_email (str): Recipient email address.
        subject (str): Email subject.
        body (str): Email body.
    """
    try:
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = constants.sender_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Connect to the Gmail SMTP server
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(constants.sender_email, constants.password)
        server.send_message(msg)
        server.quit()

        print(f"Email sent successfully to {to_email}!")
    except Exception as error:
        print(f"Failed to send the email: {error}")