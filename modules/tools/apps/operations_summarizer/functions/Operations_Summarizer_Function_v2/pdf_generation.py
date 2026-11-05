"""
PDF generation and CDF file saving functions for Operations Summarizer v2.
"""

import re
import os
from datetime import datetime, timezone, date as date_type
from cognite.client import CogniteClient
from cdf_helpers import ensure_space_exists
from config import runtime

_MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Must match the app's SUMMARY_CONTENT_TYPE_BITMAP_ORDER in summaryListQuery.ts
_BITMAP_ORDER = [
    "assets",
    "files",
    "work_orders",
    "time_series",
    "equipment",
]


def _format_date_range_display(start: date_type, end: date_type) -> str:
    """Match the app's formatDateRangeDisplay for human-readable name generation."""
    if start == end:
        return f"{_MONTHS_SHORT[start.month - 1]} {start.day}, {start.year}"
    if start.month == end.month and start.year == end.year:
        return f"{_MONTHS_SHORT[start.month - 1]} {start.day}–{end.day}, {start.year}"
    start_fmt = (
        f"{_MONTHS_SHORT[start.month - 1]} {start.day}"
        if start.year == end.year
        else f"{_MONTHS_SHORT[start.month - 1]} {start.day}, {start.year}"
    )
    return f"{start_fmt} – {_MONTHS_SHORT[end.month - 1]} {end.day}, {end.year}"


def _format_ops_bitmap_line(content_types: list) -> str:
    """Generate the OPS:{bits} prefix line matching the app's formatSummaryContentTypeBitmapLine."""
    active = set(content_types or [])
    bits = "".join("1" if t in active else "0" for t in _BITMAP_ORDER)
    return f"OPS:{bits}\n"


def _build_summary_tags(
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Build CogniteFile tags for DMS filtering."""
    tags: list[str] = []
    if start_date and end_date:
        tags.append(f"range:{start_date}_{end_date}")
    return tags


def _infer_fusion_domain(project: str, cluster: str, base_url: str) -> str | None:
    """Attempt to infer the Fusion domain based on project name, cluster, and base URL."""
    project_mappings = {
        "publicdata": "fusion.cognite.com",
    }

    if project in project_mappings:
        return project_mappings[project]

    if "-" in project:
        prefix = project.split("-")[0]
        if prefix and len(prefix) <= 20:
            return f"{prefix}.fusion.cognite.com"

    if "api.cognitedata.com" in cluster or cluster == "api.cognitedata.com":
        return "fusion.cognite.com"

    return None


def generate_pdf_from_markdown_in_memory(markdown_text: str, _filename: str) -> bytes:
    """Generate a PDF from markdown text using reportlab in memory.

    Returns the PDF as bytes.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.colors import HexColor
        from io import BytesIO
    except ImportError:
        raise ImportError(
            "reportlab is required for PDF generation. "
            "Install it with: pip install reportlab"
        )

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=40,
    )
    styles = getSampleStyleSheet()

    # Title (# heading): left-aligned bold dark navy, matches jsPDF title style
    title_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Normal"],
        fontSize=16,
        spaceAfter=8,
        spaceBefore=10,
        alignment=TA_LEFT,
        fontName="Helvetica-Bold",
        textColor=HexColor("#1a1a2e"),
    )

    # H2 (## heading): bold dark navy, 15pt above — matches jsPDF H2 style
    subheader_style = ParagraphStyle(
        "CustomSubHeader",
        parent=styles["Normal"],
        fontSize=12,
        spaceAfter=8,
        spaceBefore=15,
        fontName="Helvetica-Bold",
        textColor=HexColor("#1a1a2e"),
    )

    # H3 (### heading): bold black, 10pt above — matches jsPDF H3 style
    subsubheader_style = ParagraphStyle(
        "CustomSubSubHeader",
        parent=styles["Normal"],
        fontSize=11,
        spaceAfter=6,
        spaceBefore=10,
        fontName="Helvetica-Bold",
    )

    normal_style = ParagraphStyle(
        "CustomNormal",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=4,
        alignment=TA_LEFT,
        leftIndent=0,
    )

    # bulletIndent=5 / leftIndent=20 matches jsPDF's margin+5 / margin+20 bullet positions.
    # Using reportlab's native bulletText rendering keeps wrapped lines perfectly left-aligned.
    bullet_style_l1 = ParagraphStyle(
        "CustomBulletL1",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=2,
        leftIndent=20,
        bulletIndent=5,
    )

    bullet_style_l2 = ParagraphStyle(
        "CustomBulletL2",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=2,
        leftIndent=35,
        bulletIndent=20,
    )

    bullet_style_l3 = ParagraphStyle(
        "CustomBulletL3",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=2,
        leftIndent=50,
        bulletIndent=35,
    )

    story = []

    lines = markdown_text.split("\n")
    in_bullet_list = False

    for line in lines:
        line = line.strip()

        if not line:
            if in_bullet_list:
                in_bullet_list = False
                story.append(Spacer(1, 4))
            continue

        if line.startswith("# "):
            if in_bullet_list:
                in_bullet_list = False
            line = line[2:].strip()
            story.append(Paragraph(line, title_style))
            story.append(HRFlowable(width="100%", thickness=2, color=HexColor("#4a90a4"), spaceAfter=10))
            continue
        elif line.startswith("## "):
            if in_bullet_list:
                in_bullet_list = False
            line = line[3:].strip()
            story.append(Paragraph(line, subheader_style))
            continue
        elif line.startswith("### "):
            if in_bullet_list:
                in_bullet_list = False
            line = line[4:].strip()
            story.append(Paragraph(line, subsubheader_style))
            continue
        elif line.startswith("Summary period:"):
            if in_bullet_list:
                in_bullet_list = False
            story.append(Paragraph(f"<i>{line}</i>", normal_style))
            continue

        bullet_char = None
        if line.startswith("- ") or line.startswith("* "):
            line = line[2:].strip()
            current_style = bullet_style_l1
            bullet_char = "•"
            in_bullet_list = True
        elif line.startswith("  - ") or line.startswith("  * "):
            line = line[4:].strip()
            current_style = bullet_style_l2
            bullet_char = "◦"
            in_bullet_list = True
        elif line.startswith("    - ") or line.startswith("    * "):
            line = line[6:].strip()
            current_style = bullet_style_l3
            bullet_char = "▪"
            in_bullet_list = True
        else:
            if in_bullet_list and not line.startswith("#"):
                in_bullet_list = False
                story.append(Spacer(1, 4))
            current_style = normal_style

        line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', line)
        line = re.sub(r'\*(.*?)\*', r'<i>\1</i>', line)
        line = re.sub(r'`(.*?)`', r'<font face="Courier">\1</font>', line)
        line = line.replace("&", "&amp;")

        try:
            if bullet_char:
                story.append(Paragraph(line, current_style, bulletText=bullet_char))
            else:
                story.append(Paragraph(line, current_style))
        except Exception as e:
            print(f"Failed to create formatted paragraph: {e}")
            plain_line = re.sub(r"<[^>]+>", "", line)
            if bullet_char:
                story.append(Paragraph(plain_line, current_style, bulletText=bullet_char))
            else:
                story.append(Paragraph(plain_line, current_style))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()

    return pdf_bytes


def save_summary_to_cdf(
    client: CogniteClient,
    summary_text: str,
    start_date: str,
    end_date: str,
    generate_pdf: bool = True,
    fusion_domain: str = None,
    summary_space: str = None,
    content_types: list = None,
) -> dict:
    """Save summary to CDF as a file instance with optional PDF generation.

    Parameters
    ----------
    client : CogniteClient
    summary_text : str
    start_date : str  – "YYYY-MM-DD"
    end_date : str    – "YYYY-MM-DD"
    generate_pdf : bool
    fusion_domain : str, optional  – e.g. "cog-ariel.fusion.cognite.com"
    summary_space : str, optional  – CDF space to save into (defaults to SUMMARY_SPACE constant)
    content_types : list, optional – active content type keys for OPS bitmap
    """
    try:
        from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
    except ImportError:
        from cognite.client.data_classes.data_modeling import CogniteFileApply

    space = summary_space or runtime.summary_space

    print(f"Checking if space '{space}' exists...")
    ensure_space_exists(client, space)

    # Timestamp-based externalId matching the app's format:
    # ops_summary_{startDate}_{endDate}_{nowDate}_{nowTime}_{epochMs}
    now = datetime.now(timezone.utc)
    now_date_str = now.strftime("%Y-%m-%d")
    now_time_str = now.strftime("%H_%M_%S")
    timestamp_ms = int(now.timestamp() * 1000)
    external_id = (
        f"{runtime.operation_summary_prefix}{start_date}_{end_date}_"
        f"{now_date_str}_{now_time_str}_{timestamp_ms}"
    )

    # Human-readable name matching the app's format
    start = date_type.fromisoformat(str(start_date))
    end = date_type.fromisoformat(str(end_date))
    date_range = _format_date_range_display(start, end)
    human_readable_name = f"Operations summary, {date_range}"
    mime_type = "application/pdf" if generate_pdf else "text/markdown"
    file_ext = "pdf" if generate_pdf else "md"
    file_name = f"{human_readable_name}.{file_ext}"

    # Short metadata description with OPS bitmap prefix (matching the app)
    ops_bitmap = _format_ops_bitmap_line(content_types or [])
    content_types_str = ", ".join(content_types) if content_types else ""
    description = (
        f"{ops_bitmap}{human_readable_name}. "
        f"Content types: {content_types_str}"
    )
    tags = _build_summary_tags(
        start_date=start_date,
        end_date=end_date,
    )

    project = client.config.project
    base_url = client.config.base_url
    cluster = base_url.replace("https://", "").replace("http://", "").rstrip("/")

    if fusion_domain is None:
        fusion_domain = os.environ.get("FUSION_DOMAIN")

        if fusion_domain is None:
            fusion_domain = _infer_fusion_domain(project, cluster, base_url)

            if fusion_domain:
                print(f"Auto-detected Fusion domain: {fusion_domain}")
            else:
                raise ValueError(
                    "Fusion domain could not be auto-detected. Please provide it in one of these ways:\n"
                    "1. Pass 'fusion_domain' in the data parameter (e.g., 'cog-ariel.fusion.cognite.com')\n"
                    "2. Set FUSION_DOMAIN environment variable\n"
                    f"\nDetected project: {project}, cluster: {cluster}\n"
                )

    try:
        file_kwargs = {
            "name": file_name,
            "space": space,
            "external_id": external_id,
            "mime_type": mime_type,
            "description": description,
            "source_context": summary_text,
        }
        if tags:
            file_kwargs["tags"] = tags

        client.data_modeling.instances.apply(CogniteFileApply(**file_kwargs))

        if generate_pdf:
            pdf_bytes = generate_pdf_from_markdown_in_memory(summary_text, external_id)
            client.files.upload_content_bytes(
                content=pdf_bytes,
                instance_id=(space, external_id),
            )
            fusion_url = f"https://{fusion_domain}/{project}/search/file/cdf_cdm/v1/CogniteFile/{space}/{external_id}?cluster={cluster}&workspace=industrial-tools"
            print(f"PDF generated and uploaded to CDF: {external_id}")
            print(f"Fusion URL: {fusion_url}")
            return {"file_name": file_name, "external_id": external_id, "pdf_generated": True, "fusion_url": fusion_url}

        client.files.upload_content_bytes(
            content=summary_text.encode("utf-8"),
            instance_id=(space, external_id),
        )
        fusion_url = f"https://{fusion_domain}/{project}/search/file/cdf_cdm/v1/CogniteFile/{space}/{external_id}?cluster={cluster}&workspace=industrial-tools"
        print(f"Markdown summary uploaded to CDF: {external_id}")
        print(f"Fusion URL: {fusion_url}")
        return {"file_name": file_name, "external_id": external_id, "pdf_generated": False, "fusion_url": fusion_url}

    except Exception as e:
        print(f"Error saving summary to CDF: {e}")
        raise
