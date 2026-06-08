#!/usr/bin/env python3
"""
CV Assessor - fault-tolerant batch CV scorer.

Reads a backlog of job-application emails over IMAP, sends each attached PDF CV
directly to a Claude model (Amazon Bedrock or Anthropic API) using native PDF
ingestion, and append-writes the structured score to a CSV after every response.

Design priorities:
  - Crash-safe: each row is flushed + fsync'd immediately; a crash on email #847
    keeps the first 846 results. Reruns skip already-processed messages.
  - Robust IMAP: searches by date/ALL (never the unreliable HAS ATTACHMENT) and
    reads the real sender from the envelope From header.
  - Native PDF: raw .pdf bytes are passed straight to the model, no local parsing.
  - Pluggable provider: switch between Bedrock and Anthropic via the PROVIDER env var.

Usage:
    cp .env.example .env   # then fill in your values
    pip install -r requirements.txt
    python cv_assessor.py
"""

import base64
import csv
import email
import imaplib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from email.message import Message
from email.utils import parseaddr

from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cv_assessor")

CSV_COLUMNS = [
    "message_id",
    "candidate_email",
    "candidate_name",
    "years_experience",
    "skills_match_score",
    "red_flags",
    "two_sentence_summary",
    "source_from_header",
    "status",
    "scored_at",
]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class Config:
    def __init__(self) -> None:
        load_dotenv()
        self.provider = os.getenv("PROVIDER", "bedrock").strip().lower()

        # IMAP
        self.imap_host = os.getenv("IMAP_HOST", "").strip()
        self.imap_port = int(os.getenv("IMAP_PORT", "993"))
        self.imap_user = os.getenv("IMAP_USER", "").strip()
        self.imap_password = os.getenv("IMAP_PASSWORD", "")
        self.imap_folder = os.getenv("IMAP_FOLDER", "INBOX").strip() or "INBOX"
        self.imap_since_date = os.getenv("IMAP_SINCE_DATE", "").strip()
        self.max_emails = _int_or_none(os.getenv("MAX_EMAILS", "").strip())

        # Bedrock
        self.bedrock_endpoint_url = os.getenv("BEDROCK_ENDPOINT_URL", "").strip()
        self.aws_region = os.getenv("AWS_REGION", "us-east-1").strip()
        self.bedrock_model_id = os.getenv("BEDROCK_MODEL_ID", "").strip()

        # Anthropic
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        self.anthropic_base_url = os.getenv("ANTHROPIC_BASE_URL", "").strip()
        self.anthropic_model_id = os.getenv("ANTHROPIC_MODEL_ID", "").strip()

        # App
        self.criteria_file = os.getenv("CRITERIA_FILE", "criteria.txt").strip()
        self.output_csv = os.getenv("OUTPUT_CSV", "candidates.csv").strip()
        self.request_delay = float(os.getenv("REQUEST_DELAY_SECONDS", "1") or 0)

    def validate(self) -> None:
        missing = []
        for key in ("imap_host", "imap_user", "imap_password"):
            if not getattr(self, key):
                missing.append(key.upper())

        if self.provider == "bedrock":
            if not self.bedrock_model_id:
                missing.append("BEDROCK_MODEL_ID")
        elif self.provider == "anthropic":
            if not self.anthropic_api_key:
                missing.append("ANTHROPIC_API_KEY")
            if not self.anthropic_model_id:
                missing.append("ANTHROPIC_MODEL_ID")
        else:
            raise SystemExit(
                f"Unknown PROVIDER '{self.provider}'. Use 'bedrock' or 'anthropic'."
            )

        if missing:
            raise SystemExit(
                "Missing required configuration: " + ", ".join(missing) +
                ". Copy .env.example to .env and fill it in."
            )


def _int_or_none(value):
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
def build_prompt(criteria: str, from_email: str) -> str:
    return f"""You are a strict, experienced HR recruiter screening a CV for a role.
Be rigorous and skeptical. Do not be generous with scores.

Evaluate the attached PDF CV against the following hiring criteria:

--- HIRING CRITERIA ---
{criteria}
--- END CRITERIA ---

The candidate's email address (taken from the email envelope) is: {from_email}

Respond with ONLY a single valid JSON object and nothing else (no markdown, no
code fences, no commentary). The object MUST match exactly this schema:

{{
  "candidate_email": "string - mirror back exactly: {from_email}",
  "candidate_name": "string - the candidate's full name from the CV, or 'Unknown'",
  "years_experience": integer - total years of relevant professional experience,
  "skills_match_score": integer 0-100 - fit against the criteria (must-haves first),
  "red_flags": ["string", ...] - concrete concerns, e.g. "Job hopping", "Formatting errors", "Missing core tech stack"; empty list if none,
  "two_sentence_summary": "string - exactly two sentences assessing this candidate"
}}

Rules:
- years_experience and skills_match_score MUST be integers.
- red_flags MUST be a JSON array of strings (use [] if there are none).
- candidate_email MUST be exactly: {from_email}
- Output the JSON object only."""


# ---------------------------------------------------------------------------
# Provider adapters
# ---------------------------------------------------------------------------
class ProviderError(Exception):
    """Non-retryable provider error."""


class RetryableError(Exception):
    """Transient error (rate limit / throttling) worth retrying."""


class BedrockProvider:
    def __init__(self, cfg: Config) -> None:
        import boto3

        client_kwargs = {"region_name": cfg.aws_region}
        if cfg.bedrock_endpoint_url:
            client_kwargs["endpoint_url"] = cfg.bedrock_endpoint_url
        # AWS_BEARER_TOKEN_BEDROCK in the environment enables API-key bearer auth.
        self.client = boto3.client("bedrock-runtime", **client_kwargs)
        self.model_id = cfg.bedrock_model_id
        self._throttle_exc = self.client.exceptions.ThrottlingException

    def _raw_call(self, pdf_bytes: bytes, prompt_text: str) -> str:
        try:
            response = self.client.converse(
                modelId=self.model_id,
                messages=[{
                    "role": "user",
                    "content": [
                        {"document": {
                            "format": "pdf",
                            "name": "cv",
                            "source": {"bytes": pdf_bytes},
                        }},
                        {"text": prompt_text},
                    ],
                }],
                inferenceConfig={"maxTokens": 1024, "temperature": 0},
            )
        except self._throttle_exc as exc:
            raise RetryableError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - classify and surface
            if _looks_throttled(exc):
                raise RetryableError(str(exc)) from exc
            raise ProviderError(str(exc)) from exc

        blocks = response.get("output", {}).get("message", {}).get("content", [])
        return "".join(b.get("text", "") for b in blocks if "text" in b)

    def score_cv(self, pdf_bytes: bytes, from_email: str, criteria: str) -> dict:
        return _call_with_retry(self._raw_call, pdf_bytes, from_email, criteria)


class AnthropicProvider:
    def __init__(self, cfg: Config) -> None:
        import anthropic

        kwargs = {"api_key": cfg.anthropic_api_key}
        if cfg.anthropic_base_url:
            kwargs["base_url"] = cfg.anthropic_base_url
        self.client = anthropic.Anthropic(**kwargs)
        self.model_id = cfg.anthropic_model_id
        self._rate_limit_exc = anthropic.RateLimitError
        self._api_exc = anthropic.APIStatusError

    def _raw_call(self, pdf_bytes: bytes, prompt_text: str) -> str:
        b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")
        try:
            response = self.client.messages.create(
                model=self.model_id,
                max_tokens=1024,
                temperature=0,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "document", "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": b64,
                        }},
                        {"type": "text", "text": prompt_text},
                    ],
                }],
            )
        except self._rate_limit_exc as exc:
            raise RetryableError(str(exc)) from exc
        except self._api_exc as exc:
            if getattr(exc, "status_code", None) in (429, 500, 502, 503, 529):
                raise RetryableError(str(exc)) from exc
            raise ProviderError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise ProviderError(str(exc)) from exc

        return "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        )

    def score_cv(self, pdf_bytes: bytes, from_email: str, criteria: str) -> dict:
        return _call_with_retry(self._raw_call, pdf_bytes, from_email, criteria)


def _looks_throttled(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(s in text for s in ("throttl", "too many requests", "429", "rate exceeded"))


@retry(
    retry=retry_if_exception_type(RetryableError),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(6),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _call_with_retry(raw_call, pdf_bytes: bytes, from_email: str, criteria: str) -> dict:
    prompt_text = build_prompt(criteria, from_email)
    raw_text = raw_call(pdf_bytes, prompt_text)
    return parse_model_json(raw_text, from_email)


def make_provider(cfg: Config):
    if cfg.provider == "bedrock":
        return BedrockProvider(cfg)
    if cfg.provider == "anthropic":
        return AnthropicProvider(cfg)
    raise SystemExit(f"Unknown PROVIDER '{cfg.provider}'.")


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------
def parse_model_json(raw_text: str, from_email: str) -> dict:
    """Tolerantly extract the first JSON object and coerce to the expected schema."""
    if not raw_text:
        raise ProviderError("Empty response from model.")

    obj = _extract_json_object(raw_text)
    if obj is None:
        raise ProviderError(f"No JSON object found in response: {raw_text[:200]!r}")

    return {
        "candidate_email": str(obj.get("candidate_email") or from_email).strip(),
        "candidate_name": str(obj.get("candidate_name") or "Unknown").strip(),
        "years_experience": _coerce_int(obj.get("years_experience")),
        "skills_match_score": _coerce_int(obj.get("skills_match_score")),
        "red_flags": _coerce_str_list(obj.get("red_flags")),
        "two_sentence_summary": str(obj.get("two_sentence_summary") or "").strip(),
    }


def _extract_json_object(text: str):
    text = text.strip()
    # Strip ```json ... ``` fences if the model added them despite instructions.
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Fall back to the first balanced {...} span.
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _coerce_int(value):
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        m = re.search(r"-?\d+", value)
        if m:
            return int(m.group())
    return 0


def _coerce_str_list(value):
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


# ---------------------------------------------------------------------------
# IMAP
# ---------------------------------------------------------------------------
def connect_imap(cfg: Config) -> imaplib.IMAP4_SSL:
    log.info("Connecting to IMAP %s:%s as %s", cfg.imap_host, cfg.imap_port, cfg.imap_user)
    conn = imaplib.IMAP4_SSL(cfg.imap_host, cfg.imap_port)
    conn.login(cfg.imap_user, cfg.imap_password)
    conn.select(cfg.imap_folder)
    return conn


def search_message_ids(conn: imaplib.IMAP4_SSL, cfg: Config) -> list:
    # Deliberately NOT using (HAS ATTACHMENT) - it breaks on many servers.
    criteria = ["SINCE", cfg.imap_since_date] if cfg.imap_since_date else ["ALL"]
    typ, data = conn.search(None, *criteria)
    if typ != "OK":
        raise RuntimeError(f"IMAP search failed: {typ}")
    ids = data[0].split() if data and data[0] else []
    if cfg.max_emails is not None:
        ids = ids[: cfg.max_emails]
    return ids


def fetch_message(conn: imaplib.IMAP4_SSL, msg_id: bytes) -> Message:
    typ, data = conn.fetch(msg_id, "(RFC822)")
    if typ != "OK" or not data or not data[0]:
        raise RuntimeError(f"Failed to fetch message {msg_id!r}")
    raw_bytes = data[0][1]
    return email.message_from_bytes(raw_bytes)


def extract_from_email(msg: Message) -> str:
    _name, addr = parseaddr(msg.get("From", ""))
    return addr.strip()


def extract_message_id(msg: Message, fallback: bytes) -> str:
    mid = (msg.get("Message-ID") or "").strip()
    return mid if mid else f"UID-{fallback.decode(errors='replace')}"


def extract_first_pdf(msg: Message):
    """Return raw bytes of the first PDF attachment, or None."""
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        content_type = (part.get_content_type() or "").lower()
        filename = (part.get_filename() or "").lower()
        is_pdf = content_type == "application/pdf" or filename.endswith(".pdf")
        if not is_pdf:
            continue
        payload = part.get_payload(decode=True)
        if payload:
            return payload
    return None


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------
def load_processed_ids(path: str) -> set:
    processed = set()
    if not os.path.exists(path):
        return processed
    try:
        with open(path, "r", newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                mid = (row.get("message_id") or "").strip()
                if mid:
                    processed.add(mid)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read existing CSV for resume: %s", exc)
    return processed


def open_csv_for_append(path: str):
    is_new = not os.path.exists(path) or os.path.getsize(path) == 0
    fh = open(path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
    if is_new:
        writer.writeheader()
        fh.flush()
        os.fsync(fh.fileno())
    return fh, writer


def write_row(fh, writer, row: dict) -> None:
    full = {col: row.get(col, "") for col in CSV_COLUMNS}
    if isinstance(full.get("red_flags"), list):
        full["red_flags"] = "; ".join(full["red_flags"])
    writer.writerow(full)
    fh.flush()
    os.fsync(fh.fileno())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    cfg = Config()
    cfg.validate()

    if not os.path.exists(cfg.criteria_file):
        raise SystemExit(f"Criteria file not found: {cfg.criteria_file}")
    with open(cfg.criteria_file, "r", encoding="utf-8") as fh:
        criteria = fh.read().strip()
    if not criteria:
        raise SystemExit(f"Criteria file is empty: {cfg.criteria_file}")

    log.info("Provider: %s", cfg.provider)
    provider = make_provider(cfg)

    processed = load_processed_ids(cfg.output_csv)
    if processed:
        log.info("Resume: %d already-processed messages will be skipped.", len(processed))

    conn = None
    fh = None
    counts = {"ok": 0, "no_pdf": 0, "error": 0, "skipped": 0}
    try:
        conn = connect_imap(cfg)
        msg_ids = search_message_ids(conn, cfg)
        total = len(msg_ids)
        log.info("Found %d messages to consider.", total)

        fh, writer = open_csv_for_append(cfg.output_csv)

        for idx, msg_id in enumerate(msg_ids, start=1):
            try:
                msg = fetch_message(conn, msg_id)
            except Exception as exc:  # noqa: BLE001
                log.error("[%d/%d] fetch failed for %r: %s", idx, total, msg_id, exc)
                counts["error"] += 1
                continue

            message_id = extract_message_id(msg, msg_id)
            if message_id in processed:
                counts["skipped"] += 1
                continue

            from_email = extract_from_email(msg)
            from_header = msg.get("From", "")

            base_row = {
                "message_id": message_id,
                "candidate_email": from_email,
                "candidate_name": "",
                "years_experience": "",
                "skills_match_score": "",
                "red_flags": "",
                "two_sentence_summary": "",
                "source_from_header": from_header,
                "scored_at": now_iso(),
            }

            pdf_bytes = extract_first_pdf(msg)
            if not pdf_bytes:
                log.info("[%d/%d] no PDF for %s -> NO_PDF", idx, total, from_email or "unknown")
                write_row(fh, writer, {**base_row, "status": "NO_PDF"})
                processed.add(message_id)
                counts["no_pdf"] += 1
                continue

            try:
                result = provider.score_cv(pdf_bytes, from_email, criteria)
            except ProviderError as exc:
                log.error("[%d/%d] scoring failed for %s: %s", idx, total, from_email, exc)
                write_row(fh, writer, {
                    **base_row,
                    "status": "API_ERROR",
                    "two_sentence_summary": str(exc)[:300],
                })
                processed.add(message_id)
                counts["error"] += 1
                continue
            except Exception as exc:  # noqa: BLE001 - never abort the batch
                log.error("[%d/%d] unexpected error for %s: %s", idx, total, from_email, exc)
                write_row(fh, writer, {
                    **base_row,
                    "status": "API_ERROR",
                    "two_sentence_summary": str(exc)[:300],
                })
                processed.add(message_id)
                counts["error"] += 1
                continue

            row = {
                **base_row,
                "candidate_email": result["candidate_email"] or from_email,
                "candidate_name": result["candidate_name"],
                "years_experience": result["years_experience"],
                "skills_match_score": result["skills_match_score"],
                "red_flags": result["red_flags"],
                "two_sentence_summary": result["two_sentence_summary"],
                "status": "OK",
            }
            write_row(fh, writer, row)
            processed.add(message_id)
            counts["ok"] += 1
            log.info(
                "[%d/%d] scored %s -> %s",
                idx, total, row["candidate_email"], row["skills_match_score"],
            )

            if cfg.request_delay > 0:
                time.sleep(cfg.request_delay)

    except KeyboardInterrupt:
        log.warning("Interrupted by user. Progress is saved.")
    finally:
        if fh is not None:
            fh.close()
        if conn is not None:
            try:
                conn.logout()
            except Exception:  # noqa: BLE001
                pass

    log.info(
        "Done. OK=%d NO_PDF=%d ERROR=%d SKIPPED=%d -> %s",
        counts["ok"], counts["no_pdf"], counts["error"], counts["skipped"], cfg.output_csv,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
