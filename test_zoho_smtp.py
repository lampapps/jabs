import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

SMTP_SERVER = "smtp.zoho.com"
SMTP_PORT = 587
USERNAME = "jferry@lampapps.com"  # Change if needed
FROM_ADDR = "jferry@lampapps.com" # Change if needed
TO_ADDRS = ["jamesoferry@gmail.com"]  # Change if needed
PASSWORD = os.environ.get("JABS_EMAIL_PASSWORD")

def send_test_email():
    if not PASSWORD:
        print("No password found in JABS_EMAIL_PASSWORD.")
        return

    msg = MIMEText("This is a test email from Zoho SMTP.")
    msg['Subject'] = "Zoho SMTP Test"
    msg['From'] = FROM_ADDR
    msg['To'] = ", ".join(TO_ADDRS)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(USERNAME, PASSWORD)
            server.sendmail(FROM_ADDR, TO_ADDRS, msg.as_string())
        print("Test email sent successfully!")
    except Exception as e:
        print(f"Failed to send test email: {e}")

if __name__ == "__main__":
    send_test_email()