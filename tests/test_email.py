"""Tests for the email-parsing helpers using in-memory .eml fixtures.

We build messages with the stdlib EmailMessage and round-trip them through
email.message_from_bytes so the tests exercise the same code path the script
uses on real IMAP-fetched bytes.
"""

import email
from email.message import EmailMessage

import cv_assessor as cva

# A minimal but structurally valid single-page PDF.
SAMPLE_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]>>endobj\n"
    b"trailer<</Root 1 0 R>>\n"
    b"%%EOF\n"
)


def _roundtrip(msg: EmailMessage):
    return email.message_from_bytes(msg.as_bytes())


def _build_email(from_header, *, with_pdf=True, pdf_filename="cv.pdf",
                 pdf_subtype="pdf", extra_text=True):
    msg = EmailMessage()
    msg["From"] = from_header
    msg["To"] = "jobs@example.com"
    msg["Subject"] = "Application"
    msg["Message-ID"] = "<unit-test-123@example.com>"
    msg.set_content("Please find my CV attached.")
    if extra_text:
        msg.add_attachment(b"a text file", maintype="text", subtype="plain",
                           filename="notes.txt")
    if with_pdf:
        msg.add_attachment(SAMPLE_PDF, maintype="application", subtype=pdf_subtype,
                           filename=pdf_filename)
    return _roundtrip(msg)


def test_extract_first_pdf_found():
    msg = _build_email("Ada Lovelace <ada@cv.com>")
    assert cva.extract_first_pdf(msg) == SAMPLE_PDF


def test_extract_first_pdf_none_when_absent():
    msg = _build_email("Ada Lovelace <ada@cv.com>", with_pdf=False)
    assert cva.extract_first_pdf(msg) is None


def test_extract_first_pdf_by_extension_when_octet_stream():
    # Some mailers send the PDF as application/octet-stream; we fall back to the
    # .pdf filename extension.
    msg = _build_email("x <x@cv.com>", pdf_subtype="octet-stream", pdf_filename="resume.pdf")
    assert cva.extract_first_pdf(msg) == SAMPLE_PDF


def test_extract_from_email_strips_display_name():
    msg = _build_email("Ada Lovelace <ada@cv.com>")
    assert cva.extract_from_email(msg) == "ada@cv.com"


def test_extract_from_email_relay():
    msg = _build_email("Ada via JobBoard <noreply@jobboard.com>")
    assert cva.extract_from_email(msg) == "noreply@jobboard.com"


def test_extract_message_id_present():
    msg = _build_email("a@b.com")
    assert cva.extract_message_id(msg, b"42") == "<unit-test-123@example.com>"


def test_extract_message_id_fallback_to_uid():
    msg = EmailMessage()
    msg["From"] = "a@b.com"
    msg.set_content("no message id header")
    roundtripped = _roundtrip(msg)
    # EmailMessage may not auto-add a Message-ID; if absent we fall back to UID.
    if not roundtripped.get("Message-ID"):
        assert cva.extract_message_id(roundtripped, b"42") == "UID-42"


def test_extract_first_name_prefers_cv_name():
    msg = _build_email("Display Name <a@b.com>")
    assert cva.extract_first_name(msg, "Grace Hopper") == "Grace"


def test_extract_first_name_falls_back_to_header():
    msg = _build_email("Display Name <a@b.com>")
    assert cva.extract_first_name(msg, "Unknown") == "Display"
    assert cva.extract_first_name(msg, "") == "Display"


def test_extract_first_name_empty_when_nothing():
    msg = EmailMessage()
    msg["From"] = "a@b.com"
    msg.set_content("body")
    assert cva.extract_first_name(_roundtrip(msg), "") == ""
