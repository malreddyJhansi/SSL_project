from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from datetime import datetime
from utils.logger_utils import logger

error_style = ParagraphStyle(name="ErrorStyle", fontName="Helvetica", fontSize=8, leading=10, alignment=TA_LEFT)

def build_table(data, headers, header_color=colors.grey, col_widths=None):
    table_data = [headers] + data
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table

def generate_ssl_report(expired, expiring, invalid, output_file="ssl_report.pdf"):
    doc = SimpleDocTemplate(output_file, pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("SSL Certificate Report", styles["Title"]))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
    elements.append(Spacer(1, 20))

    summary = f"<b>Summary</b><br/>Expired: <b>{len(expired)}</b><br/>Expiring Soon: <b>{len(expiring)}</b><br/>Invalid: <b>{len(invalid)}</b><br/>"
    elements.append(Paragraph(summary, styles["Normal"]))
    elements.append(Spacer(1, 20))

    sections = [
        ("Expired Certificates", expired, ["Hostname", "Port", "Issuer", "Expiry Date"], colors.red, [170, 50, 150, 120]),
        ("Expiring Soon Certificates", expiring, ["Hostname", "Port", "Issuer", "Expiry Date", "Days Left"], colors.orange, [170, 50, 150, 100, 60]),
        ("Invalid Certificates", invalid, ["Hostname", "Port", "Error Message"], colors.grey, [170, 50, 300])
    ]

    for i, (title, data, headers, color, widths) in enumerate(sections):
        elements.append(Paragraph(f"{title} (Count: {len(data)})", styles["Heading2"]))
        if data:
            if "Error Message" in headers:
                table_data = [[e.get("hostname", "N/A"), e.get("port", "N/A"), Paragraph(e.get("error_message", "N/A"), error_style)] for e in data]
            elif "Days Left" in headers:
                table_data = [[e.get("hostname", "N/A"), e.get("port", "N/A"), e.get("cert_issuer", "N/A"), e.get("valid_to", "N/A"), e.get("days_to_expiry", "N/A")] for e in data]
            else:
                table_data = [[e.get("hostname", "N/A"), e.get("port", "N/A"), e.get("cert_issuer", "N/A"), e.get("valid_to", "N/A")] for e in data]
            elements.append(build_table(table_data, headers, header_color=color, col_widths=widths))
        else:
            elements.append(Paragraph(f"No {title.lower()}.", styles["Normal"]))
        if i < len(sections) - 1:
            elements.append(Spacer(1, 20))

    doc.build(elements)
    logger.info(f"✅ SSL Report generated: {output_file}")
