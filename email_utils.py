import os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from utils.logger_utils import logger

def build_email(sender, recipient, subject, body, attachment_path=None):
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            mime_base = MIMEBase("application", "octet-stream")
            mime_base.set_payload(f.read())
            encoders.encode_base64(mime_base)
            mime_base.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(mime_base)
    return msg

def send_email(msg, smtp_server, smtp_port, username, password):
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
        logger.info("✅ Email sent successfully!")
    except Exception as e:
        logger.error("❌ Email sending failed", exc_info=True)

