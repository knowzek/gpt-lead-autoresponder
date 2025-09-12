# emailer.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASS = os.getenv("GMAIL_APP_PASSWORD")

def send_email(to, subject, body, attachments=None):
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    if isinstance(to, list):
        msg['To'] = ", ".join(to)
    else:
        msg['To'] = to

    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    # Attach files if provided
    if attachments:
        from email.mime.base import MIMEBase
        from email import encoders
        for path in attachments:
            try:
                with open(path, "rb") as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                import os as _os
                part.add_header('Content-Disposition', f'attachment; filename="{_os.path.basename(path)}"')
                msg.attach(part)
            except Exception as e:
                print(f"⚠️ Failed to attach {path}: {e}")

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASS)
        server.sendmail(GMAIL_USER, [to] if isinstance(to, str) else to, msg.as_string())
        server.quit()
        print(f"✅ Email sent to {to}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
