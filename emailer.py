# emailer.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASS = os.getenv("GMAIL_PASS")

def send_email(to, subject, body):
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    if isinstance(to, list):
        msg['To'] = ", ".join(to)
    else:
        msg['To'] = to

    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASS)
        server.sendmail(GMAIL_USER, [to] if isinstance(to, str) else to, msg.as_string())
        server.quit()
        print(f"✅ Email sent to {to}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
