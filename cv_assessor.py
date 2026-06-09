#!/usr/bin/env python3
"""
CV Assessor - fault-tolerant batch CV scorer.

Reads a backlog of job-application emails over IMAP, sends each attached PDF CV
directly to a Claude model (Amazon Bedrock, Anthropic API, or an OpenAI-compatible
AI gateway such as LiteLLM) using native PDF ingestion, and append-writes the
structured score to a CSV after every response.

Design priorities:
  - Crash-safe: each row is flushed + fsync'd immediately; a crash on email #847
    keeps the first 846 results. Reruns skip already-processed messages.
  - Robust IMAP: searches by date/ALL (never the unreliable HAS ATTACHMENT) and
    reads the real sender from the envelope From header.
  - Native PDF: raw .pdf bytes are passed straight to the model, no local parsing.
  - Pluggable provider: switch between Bedrock, Anthropic, and an OpenAI-compatible
    gateway via the PROVIDER env var.

Usage:
    cp .env.example .env   # then fill in your values
    pip install -e ".[gateway]"   # or [bedrock] / [anthropic] / [all]
    python cv_assessor.py
    # or, after install, the console entry point:
    cv-assessor --provider gateway --limit 10 --dry-run

CLI flags override the matching .env values (see --help).
"""

import argparse
import base64
import csv
import email
import imaplib
import json
import logging
import os
import re
import smtplib
import sys
import time
from collections import deque
from datetime import datetime, timezone
from email.message import EmailMessage, Message
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

__version__ = "0.1.0"

CSV_COLUMNS = [
    "message_id",
    "candidate_email",
    "candidate_phone",
    "candidate_name",
    "years_experience",
    "skills_match_score",
    "red_flags",
    "two_sentence_summary",
    "source_from_header",
    "status",
    "ack_status",
    "scored_at",
]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class Config:
    def __init__(self) -> None:
        load_dotenv()
        self.dry_run = False
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

        # Gateway (LiteLLM / OpenAI-compatible)
        self.gateway_base_url = os.getenv("GATEWAY_BASE_URL", "").strip()
        self.gateway_api_key = os.getenv("GATEWAY_API_KEY", "").strip()
        self.gateway_model_id = os.getenv("GATEWAY_MODEL_ID", "").strip()

        # App
        self.criteria_file = os.getenv("CRITERIA_FILE", "criteria.txt").strip()
        self.output_csv = os.getenv("OUTPUT_CSV", "candidates.csv").strip()
        self.request_delay = float(os.getenv("REQUEST_DELAY_SECONDS", "1") or 0)

        # Acknowledgement email (opt-in)
        self.send_ack = os.getenv("SEND_ACK", "false").strip().lower() == "true"
        self.smtp_host = os.getenv("SMTP_HOST", "").strip()
        self.smtp_port = int(os.getenv("SMTP_PORT", "587") or 587)
        self.smtp_user = os.getenv("SMTP_USER", "").strip()
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.ack_from = os.getenv("ACK_FROM", "").strip()
        self.ack_subject = os.getenv("ACK_SUBJECT", "We've received your application").strip()
        self.ack_template_file = os.getenv(
            "ACK_TEMPLATE_FILE", "response_email_template.txt"
        ).strip()
        # Rolling-window send limiter: at most ACK_RATE_LIMIT acknowledgement
        # sends per ACK_RATE_PERIOD_SECONDS. Set ACK_RATE_LIMIT=0 to disable.
        self.ack_rate_limit = int(os.getenv("ACK_RATE_LIMIT", "139") or 0)
        self.ack_rate_period = float(os.getenv("ACK_RATE_PERIOD_SECONDS", "3600") or 3600)

    def apply_overrides(self, args: argparse.Namespace) -> None:
        """Apply CLI flags on top of the .env-derived values (flags win)."""
        if getattr(args, "provider", None):
            self.provider = args.provider.strip().lower()
        if getattr(args, "since", None) is not None:
            self.imap_since_date = args.since.strip()
        if getattr(args, "limit", None) is not None:
            self.max_emails = args.limit
        if getattr(args, "criteria_file", None):
            self.criteria_file = args.criteria_file.strip()
        if getattr(args, "output_csv", None):
            self.output_csv = args.output_csv.strip()
        if getattr(args, "dry_run", False):
            self.dry_run = True

    def validate(self) -> None:
        missing = []
        for key in ("imap_host", "imap_user", "imap_password"):
            if not getattr(self, key):
                missing.append(key.upper())

        # A dry run never calls the model or sends mail, so provider and SMTP
        # credentials are not required - only IMAP access to list the backlog.
        if self.dry_run:
            if self.provider not in ("bedrock", "anthropic", "gateway"):
                raise SystemExit(
                    f"Unknown PROVIDER '{self.provider}'. "
                    "Use 'bedrock', 'anthropic', or 'gateway'."
                )
            if missing:
                raise SystemExit(
                    "Missing required configuration: " + ", ".join(missing) +
                    ". Copy .env.example to .env and fill it in."
                )
            return

        if self.provider == "bedrock":
            if not self.bedrock_model_id:
                missing.append("BEDROCK_MODEL_ID")
        elif self.provider == "anthropic":
            if not self.anthropic_api_key:
                missing.append("ANTHROPIC_API_KEY")
            if not self.anthropic_model_id:
                missing.append("ANTHROPIC_MODEL_ID")
        elif self.provider == "gateway":
            if not self.gateway_base_url:
                missing.append("GATEWAY_BASE_URL")
            if not self.gateway_api_key:
                missing.append("GATEWAY_API_KEY")
            if not self.gateway_model_id:
                missing.append("GATEWAY_MODEL_ID")
        else:
            raise SystemExit(
                f"Unknown PROVIDER '{self.provider}'. "
                "Use 'bedrock', 'anthropic', or 'gateway'."
            )

        if self.send_ack:
            for key in ("smtp_host", "smtp_user", "smtp_password", "ack_from"):
                if not getattr(self, key):
                    missing.append(key.upper())

        if missing:
            raise SystemExit(
                "Missing required configuration: " + ", ".join(missing) +
                ". Copy .env.example to .env and fill it in."
            )

        if self.send_ack:
            if not os.path.exists(self.ack_template_file):
                raise SystemExit(
                    f"Acknowledgement template file not found: {self.ack_template_file}"
                )
            with open(self.ack_template_file, "r", encoding="utf-8") as fh:
                if not fh.read().strip():
                    raise SystemExit(
                        f"Acknowledgement template file is empty: {self.ack_template_file}"
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

The email arrived from this envelope address: {from_email}
NOTE: this may be a job-board / advertising relay (e.g. "Name via somesite.com"),
NOT the candidate's real address. Always prefer the email written inside the CV.

Respond with ONLY a single valid JSON object and nothing else (no markdown, no
code fences, no commentary). The object MUST match exactly this schema:

{{
  "candidate_email": "string - the candidate's OWN email address as written in the CV. Only if the CV contains no email at all, fall back to the envelope address {from_email}",
  "candidate_phone": "string - the candidate's phone/mobile number exactly as written in the CV, or '' if not found",
  "candidate_name": "string - the candidate's full name from the CV, or 'Unknown'",
  "years_experience": integer - total years of relevant professional experience,
  "skills_match_score": integer 0-100 - fit against the criteria (must-haves first),
  "red_flags": ["string", ...] - concrete concerns, e.g. "Job hopping", "Formatting errors", "Missing core tech stack"; empty list if none,
  "two_sentence_summary": "string - exactly two sentences assessing this candidate"
}}

Rules:
- years_experience and skills_match_score MUST be integers.
- red_flags MUST be a JSON array of strings (use [] if there are none).
- candidate_email MUST be the candidate's own email taken from the CV; use the
  envelope address {from_email} only when the CV contains no email whatsoever.
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


class GatewayProvider:
    """OpenAI-compatible gateway (e.g. LiteLLM) with native PDF via file data-URI."""

    def __init__(self, cfg: Config) -> None:
        import openai

        self.client = openai.OpenAI(
            api_key=cfg.gateway_api_key,
            base_url=cfg.gateway_base_url,
        )
        self.model_id = cfg.gateway_model_id
        self._rate_limit_exc = openai.RateLimitError
        self._api_exc = openai.APIStatusError

    def _raw_call(self, pdf_bytes: bytes, prompt_text: str) -> str:
        b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")
        data_uri = f"data:application/pdf;base64,{b64}"
        try:
            response = self.client.chat.completions.create(
                model=self.model_id,
                max_tokens=1024,
                temperature=0,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "file", "file": {
                            "filename": "cv.pdf",
                            "file_data": data_uri,
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

        return response.choices[0].message.content or ""

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
    if cfg.provider == "gateway":
        return GatewayProvider(cfg)
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
        "candidate_phone": str(obj.get("candidate_phone") or "").strip(),
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


def extract_first_name(msg: Message, candidate_name: str = "") -> str:
    """Best-effort first name: prefer the scored CV name, fall back to the
    display name in the From header, else empty."""
    name = (candidate_name or "").strip()
    if name and name.lower() != "unknown":
        return name.split()[0]
    display, _addr = parseaddr(msg.get("From", ""))
    display = display.strip()
    if display:
        return display.split()[0]
    return ""


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
# Acknowledgement email
# ---------------------------------------------------------------------------
def render_ack_body(template: str, first_name: str) -> str:
    name = (first_name or "").strip() or "there"
    return template.replace("[first_name]", name)


class RateLimiter:
    """Rolling-window limiter: allows at most `limit` events per `period` seconds.

    `acquire()` blocks (sleeps) when the cap is reached, until the oldest event
    ages out of the window, then records the new event. Keyed off a monotonic
    clock so it is immune to wall-clock adjustments. State is in-memory only, so
    the window resets if the process is restarted.
    """

    def __init__(self, limit: int, period: float) -> None:
        self.limit = limit
        self.period = period
        self._events = deque()

    def _purge(self, now: float) -> None:
        while self._events and now - self._events[0] >= self.period:
            self._events.popleft()

    def acquire(self) -> None:
        if self.limit <= 0:
            return
        now = time.monotonic()
        self._purge(now)
        if len(self._events) >= self.limit:
            sleep_for = self.period - (now - self._events[0])
            if sleep_for > 0:
                log.info(
                    "Acknowledgement rate cap reached (%d per %.0fs); sleeping %.0fs.",
                    self.limit, self.period, sleep_for,
                )
                time.sleep(sleep_for)
            self._purge(time.monotonic())
        self._events.append(time.monotonic())


# Connection-level failures worth a reconnect/retry (NOT recipient/auth errors).
SMTP_CONNECTION_ERRORS = (
    smtplib.SMTPServerDisconnected,
    smtplib.SMTPConnectError,
    smtplib.SMTPHeloError,
    OSError,  # socket errors, timeouts, connection reset, DNS, etc.
)

SMTP_CONNECT_TIMEOUT = 30


@retry(
    retry=retry_if_exception_type(SMTP_CONNECTION_ERRORS),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _smtp_connect(cfg: "Config") -> smtplib.SMTP:
    """Open + STARTTLS + login, retrying transient connection failures.
    Authentication errors are NOT retried (they re-raise immediately)."""
    log.info("Connecting to SMTP %s:%s as %s", cfg.smtp_host, cfg.smtp_port, cfg.smtp_user)
    smtp = smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=SMTP_CONNECT_TIMEOUT)
    try:
        smtp.starttls()
        smtp.login(cfg.smtp_user, cfg.smtp_password)
    except Exception:
        try:
            smtp.close()
        except Exception:  # noqa: BLE001
            pass
        raise
    log.info("SMTP connection established.")
    return smtp


class SmtpMailer:
    """Holds one reusable SMTP connection that transparently reconnects when the
    server drops it part-way through a long batch."""

    def __init__(self, cfg: "Config") -> None:
        self.cfg = cfg
        self.smtp = None

    def connect(self) -> None:
        self.smtp = _smtp_connect(self.cfg)

    def _drop(self) -> None:
        if self.smtp is not None:
            try:
                self.smtp.close()
            except Exception:  # noqa: BLE001
                pass
            self.smtp = None

    def close(self) -> None:
        if self.smtp is not None:
            try:
                self.smtp.quit()
            except Exception:  # noqa: BLE001
                pass
            self.smtp = None

    def send(self, to_email: str, body: str, in_reply_to: str = None) -> None:
        """Send one ack. On a dropped connection, reconnect once and resend.
        Raises if it still fails (caller records FAILED)."""
        msg = EmailMessage()
        msg["From"] = self.cfg.ack_from
        msg["To"] = to_email
        msg["Subject"] = self.cfg.ack_subject
        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
            msg["References"] = in_reply_to
        msg.set_content(body)

        attempts = 2
        for attempt in range(1, attempts + 1):
            try:
                if self.smtp is None:
                    self.connect()
                self.smtp.send_message(msg)
                return
            except SMTP_CONNECTION_ERRORS as exc:
                # Connection-level problem: drop the dead socket and reconnect.
                log.warning(
                    "SMTP connection lost while sending to %s (attempt %d/%d): %s",
                    to_email, attempt, attempts, exc,
                )
                self._drop()
                if attempt == attempts:
                    raise


def attempt_ack(mailer, cfg: "Config", to_email: str, template: str,
                first_name: str, in_reply_to: str = None, limiter=None) -> str:
    """Guarded send. Returns ack_status: SENT / FAILED / SKIPPED. Never raises.

    The rate limiter is consumed only for real send attempts (after the SKIPPED
    guards), so skipped messages never count against the hourly budget. A send
    that ends up FAILED still consumes a slot, since the server may have accepted
    the message before the failure - this keeps us safely under the cap."""
    if not cfg.send_ack or mailer is None or not (to_email or "").strip():
        return "SKIPPED"
    try:
        body = render_ack_body(template, first_name)
        if limiter is not None:
            limiter.acquire()
        mailer.send(to_email, body, in_reply_to)
        return "SENT"
    except Exception as exc:  # noqa: BLE001 - a send failure must not abort the batch
        log.error("Acknowledgement send failed for %s: %s", to_email, exc)
        return "FAILED"


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


def _tally_ack(counts: dict, ack_status: str) -> None:
    if ack_status == "SENT":
        counts["ack_sent"] += 1
    elif ack_status == "FAILED":
        counts["ack_failed"] += 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cv-assessor",
        description=(
            "Batch-score job-application PDF CVs from an IMAP inbox with a Claude "
            "model. CLI flags override the matching .env values."
        ),
        epilog=(
            "Reminder: output is decision-support only and must be reviewed by a "
            "human. Do not use it to automatically reject candidates."
        ),
    )
    parser.add_argument(
        "--provider",
        choices=["bedrock", "anthropic", "gateway"],
        help="Model backend to use (overrides PROVIDER).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process at most N emails this run (overrides MAX_EMAILS).",
    )
    parser.add_argument(
        "--since",
        metavar="DD-Mon-YYYY",
        help="Only fetch mail on/after this date, e.g. 01-Jan-2026 (overrides IMAP_SINCE_DATE).",
    )
    parser.add_argument(
        "--criteria-file",
        dest="criteria_file",
        help="Path to the hiring-criteria file (overrides CRITERIA_FILE).",
    )
    parser.add_argument(
        "--output-csv",
        dest="output_csv",
        help="Path to the output CSV (overrides OUTPUT_CSV).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "List what would be processed (which emails have a PDF CV) without "
            "calling the model, sending acknowledgements, or writing the CSV. "
            "Only IMAP access is required."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser.parse_args(argv)


def run_dry(cfg: Config) -> int:
    """List the backlog and whether each message has a PDF CV. No model calls,
    no acknowledgements, no CSV writes."""
    processed = load_processed_ids(cfg.output_csv)
    if processed:
        log.info("Resume: %d already-processed messages would be skipped.", len(processed))

    conn = None
    counts = {"would_score": 0, "no_pdf": 0, "already_done": 0}
    try:
        conn = connect_imap(cfg)
        msg_ids = search_message_ids(conn, cfg)
        total = len(msg_ids)
        log.info("DRY RUN: found %d messages to consider (provider=%s).", total, cfg.provider)
        for idx, msg_id in enumerate(msg_ids, start=1):
            try:
                msg = fetch_message(conn, msg_id)
            except Exception as exc:  # noqa: BLE001
                log.error("[%d/%d] fetch failed for %r: %s", idx, total, msg_id, exc)
                continue
            message_id = extract_message_id(msg, msg_id)
            from_email = extract_from_email(msg) or "unknown"
            if message_id in processed:
                counts["already_done"] += 1
                log.info("[%d/%d] %s -> ALREADY PROCESSED (skip)", idx, total, from_email)
                continue
            if extract_first_pdf(msg):
                counts["would_score"] += 1
                log.info("[%d/%d] %s -> WOULD SCORE (PDF found)", idx, total, from_email)
            else:
                counts["no_pdf"] += 1
                log.info("[%d/%d] %s -> NO_PDF (skip)", idx, total, from_email)
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
    finally:
        if conn is not None:
            try:
                conn.logout()
            except Exception:  # noqa: BLE001
                pass

    log.info(
        "DRY RUN complete. WOULD_SCORE=%d NO_PDF=%d ALREADY_PROCESSED=%d (nothing written).",
        counts["would_score"], counts["no_pdf"], counts["already_done"],
    )
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = Config()
    cfg.apply_overrides(args)
    cfg.validate()

    if cfg.dry_run:
        if not os.path.exists(cfg.criteria_file):
            log.warning("Criteria file not found: %s (not needed for --dry-run).", cfg.criteria_file)
        log.info("Provider: %s (DRY RUN - no model calls will be made).", cfg.provider)
        return run_dry(cfg)

    if not os.path.exists(cfg.criteria_file):
        raise SystemExit(f"Criteria file not found: {cfg.criteria_file}")
    with open(cfg.criteria_file, "r", encoding="utf-8") as fh:
        criteria = fh.read().strip()
    if not criteria:
        raise SystemExit(f"Criteria file is empty: {cfg.criteria_file}")

    ack_template = ""
    ack_limiter = None
    if cfg.send_ack:
        with open(cfg.ack_template_file, "r", encoding="utf-8") as fh:
            ack_template = fh.read()
        log.info("Acknowledgement emails ENABLED (from %s).", cfg.ack_from)
        if cfg.ack_rate_limit > 0:
            ack_limiter = RateLimiter(cfg.ack_rate_limit, cfg.ack_rate_period)
            log.info(
                "Acknowledgement rate limit: max %d sends per %.0fs.",
                cfg.ack_rate_limit, cfg.ack_rate_period,
            )
        else:
            log.info("Acknowledgement rate limit disabled (ACK_RATE_LIMIT=0).")
    else:
        log.info("Acknowledgement emails disabled (SEND_ACK is not true).")

    log.info("Provider: %s", cfg.provider)
    provider = make_provider(cfg)

    processed = load_processed_ids(cfg.output_csv)
    if processed:
        log.info("Resume: %d already-processed messages will be skipped.", len(processed))

    conn = None
    fh = None
    mailer = None
    counts = {"ok": 0, "no_pdf": 0, "error": 0, "skipped": 0,
              "ack_sent": 0, "ack_failed": 0}
    try:
        conn = connect_imap(cfg)
        msg_ids = search_message_ids(conn, cfg)
        total = len(msg_ids)
        log.info("Found %d messages to consider.", total)

        fh, writer = open_csv_for_append(cfg.output_csv)

        if cfg.send_ack:
            mailer = SmtpMailer(cfg)
            mailer.connect()

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
                "candidate_phone": "",
                "candidate_name": "",
                "years_experience": "",
                "skills_match_score": "",
                "red_flags": "",
                "two_sentence_summary": "",
                "source_from_header": from_header,
                "ack_status": "",
                "scored_at": now_iso(),
            }

            pdf_bytes = extract_first_pdf(msg)
            if not pdf_bytes:
                # No CV attached -> nothing to acknowledge per scope.
                log.info("[%d/%d] no PDF for %s -> NO_PDF", idx, total, from_email or "unknown")
                write_row(fh, writer, {
                    **base_row, "status": "NO_PDF", "ack_status": "SKIPPED",
                })
                processed.add(message_id)
                counts["no_pdf"] += 1
                continue

            try:
                result = provider.score_cv(pdf_bytes, from_email, criteria)
            except Exception as exc:  # noqa: BLE001 - never abort the batch
                if isinstance(exc, ProviderError):
                    log.error("[%d/%d] scoring failed for %s: %s", idx, total, from_email, exc)
                else:
                    log.error("[%d/%d] unexpected error for %s: %s", idx, total, from_email, exc)
                # A CV was attached, so acknowledge regardless of the scoring failure.
                row = {
                    **base_row,
                    "status": "API_ERROR",
                    "two_sentence_summary": str(exc)[:300],
                }
                ack_status = attempt_ack(
                    mailer, cfg, from_email, ack_template,
                    extract_first_name(msg), in_reply_to=message_id,
                    limiter=ack_limiter,
                )
                row["ack_status"] = ack_status
                _tally_ack(counts, ack_status)
                write_row(fh, writer, row)
                processed.add(message_id)
                counts["error"] += 1
                continue

            row = {
                **base_row,
                "candidate_email": result["candidate_email"] or from_email,
                "candidate_phone": result["candidate_phone"],
                "candidate_name": result["candidate_name"],
                "years_experience": result["years_experience"],
                "skills_match_score": result["skills_match_score"],
                "red_flags": result["red_flags"],
                "two_sentence_summary": result["two_sentence_summary"],
                "status": "OK",
            }
            ack_status = attempt_ack(
                mailer, cfg, row["candidate_email"], ack_template,
                extract_first_name(msg, result["candidate_name"]),
                in_reply_to=message_id, limiter=ack_limiter,
            )
            row["ack_status"] = ack_status
            _tally_ack(counts, ack_status)
            write_row(fh, writer, row)
            processed.add(message_id)
            counts["ok"] += 1
            log.info(
                "[%d/%d] scored %s -> %s (ack=%s)",
                idx, total, row["candidate_email"], row["skills_match_score"], ack_status,
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
        if mailer is not None:
            mailer.close()

    log.info(
        "Done. OK=%d NO_PDF=%d ERROR=%d SKIPPED=%d ACK_SENT=%d ACK_FAILED=%d -> %s",
        counts["ok"], counts["no_pdf"], counts["error"], counts["skipped"],
        counts["ack_sent"], counts["ack_failed"], cfg.output_csv,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
