import json, os
from datetime import datetime
from utils.logger_utils import logger
from utils.spark_utils import init_spark, process_domains
from utils.pdf_report import generate_ssl_report
from utils.email_utils import build_email, send_email

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

SMTP_SERVER = config["smtp_server"]
SMTP_PORT = config["smtp_port"]
SMTP_USER = config["smtp_user"]
SMTP_PASS = config["smtp_pass"]
SENDER_EMAIL = config["sender_email"]
RECIPIENT_EMAIL = config["recipient_email"]
EXPIRY_THRESHOLD = config.get("expiry_threshold", 30)

def main():
    logger.info("🚀 Starting SSL Monitoring Job")

    spark = init_spark()
    expired, expiring, invalid = process_domains(spark, EXPIRY_THRESHOLD)

    pdf_path = f"ssl_report_{datetime.now().strftime('%d%m%Y')}.pdf"
    generate_ssl_report(expired, expiring, invalid, pdf_path)

    email_msg = build_email(
        sender=SENDER_EMAIL,
        recipient=RECIPIENT_EMAIL,
        subject="SSL Certificate Report",
        body="Hello Admin,\n\nPlease find attached the latest SSL Certificate Report.\n\nRegards,\nSSL Monitoring System",
        attachment_path=pdf_path
    )

    send_email(email_msg, SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASS)
    logger.info("🏁 Job completed successfully!")

if __name__ == "__main__":
    main()
