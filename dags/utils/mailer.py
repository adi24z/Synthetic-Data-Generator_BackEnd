import os
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")


def send_email(recipient, download_url):

    subject = "Synthetic Dataset Download Link"

    body = f"""
Hello,

Your synthetic dataset has been generated successfully.

Download Link:
{download_url}

This link may expire in 1 hour.

Regards,
AI Data Pipeline
"""

    msg = MIMEMultipart()

    msg["From"] = EMAIL_SENDER
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:

        server.starttls()

        server.login(
            EMAIL_SENDER,
            EMAIL_PASSWORD
        )

        server.send_message(msg)

    print("EMAIL SENT SUCCESSFULLY")