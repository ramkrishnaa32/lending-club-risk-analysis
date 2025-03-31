import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import lib.constants as constants

def read_from_file(spark, file_path):
    """
    Reads data from a file.
    Parameters:
        spark (SparkSession): The Spark session object.
        file_path (str): The path to the file.

    Returns:
        DataFrame: A DataFrame containing the data from the file.
    """
    df = spark.read.format("csv").option("header", "true").load(file_path)
    return df

def send_email(to_email, subject, body, attachment_path=None):
    """
    Sends an email notification with an optional CSV attachment.

    Parameters:
        to_email (str): Recipient email address.
        subject (str): Email subject.
        body (str): Email body.
        attachment_path (str, optional): Path to the CSV file to be attached.
    """
    try:
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = constants.sender_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Attach CSV file if provided
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as file:
                attachment = MIMEBase("application", "octet-stream")
                attachment.set_payload(file.read())
                encoders.encode_base64(attachment)
                attachment.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(attachment_path)}"
                )
                msg.attach(attachment)
            print(f"Attached file: {attachment_path}")

        # Connect to Gmail SMTP server
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(constants.sender_email, constants.password)
        server.send_message(msg)
        server.quit()

        print(f"Email sent successfully to {to_email}!")
    except Exception as error:
        print(f"Failed to send the email: {error}")