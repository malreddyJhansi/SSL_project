import ssl, socket, re
from datetime import datetime, timezone
from utils.logger_utils import logger

def is_valid_hostname(hostname: str) -> bool:
    pattern = re.compile(r"^(?=.{1,253}$)(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.(?!-)[A-Za-z0-9-]{1,63}(?<!-))*$")
    return bool(hostname and pattern.match(hostname))

def classify_error(hostname, error_msg):
    err = str(error_msg or "").lower()
    if not is_valid_hostname(hostname): return "DATA_INPUT_ERROR", "Invalid Hostname"
    if "name or service not known" in err: return "DATA_INPUT_ERROR", "Nonexistent Domain"
    if "timed out" in err or "refused" in err: return "NETWORK_ERROR", "Unreachable"
    if "certificate has expired" in err: return "SSL_CERT_ERROR", "Expired"
    if "self signed" in err: return "SSL_CERT_ERROR", "Self-signed"
    if "hostname mismatch" in err or "doesn't match" in err: return "SSL_CERT_ERROR", "Wrong Hostname"
    if "certificate verify failed" in err: return "SSL_CERT_ERROR", "Untrusted Root"
    return "UNKNOWN_ERROR", "Fetch Failed"

def fetch_ssl_cert(hostname: str, port: int, expiry_threshold: int = 30):
    retries = 2
    last_error = None

    for attempt in range(retries):
        try:
            context = ssl.create_default_context()
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

            with socket.create_connection((hostname, port), timeout=5) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    cert = ssock.getpeercert()
                    not_before = datetime.strptime(cert['notBefore'], '%b %d %H:%M:%S %Y %Z')
                    not_after = datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z').replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    days_to_expiry = (not_after - now).days

                    if now > not_after:
                        status = "expired"
                    elif days_to_expiry <= expiry_threshold:
                        status = "expiring soon"
                    else:
                        status = "valid"

                    issuer = dict(x[0] for x in cert['issuer']).get('organizationName', 'N/A')
                    subject = dict(x[0] for x in cert['subject']).get('commonName', 'N/A')

                    return (
                        True, issuer, subject,
                        not_before.strftime('%Y-%m-%d'),
                        not_after.strftime('%Y-%m-%d'),
                        days_to_expiry, status,
"SSL_CERT_OK", "", "NO_ALERT"
                    )
        except Exception as e:
            last_error = str(e)

    issue_category, cert_status = classify_error(hostname, last_error or "Unknown failure")
    # ✅ Normalize and simplify status before returning
    status = (cert_status or "invalid").strip().lower()
    
    if "expired" in status:
        status = "expired"
    elif "expiring" in status:
        status = "expiring soon"
    elif "valid" in status:
        status = "valid"
    else:
        status = "invalid"
    return (False, "N/A", "N/A", "N/A", "N/A", None, cert_status, issue_category, last_error, "INVALID_ALERT")
