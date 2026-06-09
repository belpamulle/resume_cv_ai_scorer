"""Unit tests for the pure, side-effect-free helpers in cv_assessor.

These cover the parsing/coercion layer (the part most likely to break when a
model returns slightly-off JSON) plus email PDF extraction and argument parsing.
None of these tests touch the network, IMAP, SMTP, or any provider SDK.
"""

import pytest

import cv_assessor as cva


# ---------------------------------------------------------------------------
# _coerce_int
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value, expected",
    [
        (5, 5),
        (0, 0),
        (-3, -3),
        (4.9, 4),
        ("7", 7),
        ("about 12 years", 12),
        ("-2 net", -2),
        ("none", 0),
        ("", 0),
        (None, 0),
        (True, 0),   # bool must not be treated as 1
        (False, 0),
        ([], 0),
    ],
)
def test_coerce_int(value, expected):
    assert cva._coerce_int(value) == expected


# ---------------------------------------------------------------------------
# _coerce_str_list
# ---------------------------------------------------------------------------
def test_coerce_str_list_from_list():
    assert cva._coerce_str_list(["a", " b ", "", "c"]) == ["a", "b", "c"]


def test_coerce_str_list_from_string():
    assert cva._coerce_str_list("single flag") == ["single flag"]


def test_coerce_str_list_empty_inputs():
    assert cva._coerce_str_list(None) == []
    assert cva._coerce_str_list("") == []
    assert cva._coerce_str_list("   ") == []
    assert cva._coerce_str_list([]) == []


def test_coerce_str_list_stringifies_non_strings():
    assert cva._coerce_str_list([1, 2.5, None]) == ["1", "2.5", "None"]


# ---------------------------------------------------------------------------
# _extract_json_object
# ---------------------------------------------------------------------------
def test_extract_plain_json():
    assert cva._extract_json_object('{"a": 1}') == {"a": 1}


def test_extract_json_with_code_fence():
    text = 'Here you go:\n```json\n{"a": 1, "b": [2, 3]}\n```\nthanks'
    assert cva._extract_json_object(text) == {"a": 1, "b": [2, 3]}


def test_extract_json_with_leading_and_trailing_prose():
    text = 'Sure! {"score": 80} Hope that helps.'
    assert cva._extract_json_object(text) == {"score": 80}


def test_extract_json_nested_braces():
    text = 'noise {"outer": {"inner": 1}} noise'
    assert cva._extract_json_object(text) == {"outer": {"inner": 1}}


def test_extract_json_none_when_absent():
    assert cva._extract_json_object("no json here") is None


def test_extract_json_none_when_malformed():
    assert cva._extract_json_object("{not valid json") is None


# ---------------------------------------------------------------------------
# parse_model_json
# ---------------------------------------------------------------------------
FALLBACK = "envelope@example.com"


def test_parse_model_json_happy_path():
    raw = """{
        "candidate_email": "me@cv.com",
        "candidate_phone": "+61 400 000 000",
        "candidate_name": "Ada Lovelace",
        "years_experience": 9,
        "skills_match_score": 88,
        "red_flags": ["Job hopping"],
        "two_sentence_summary": "Strong. Hire."
    }"""
    result = cva.parse_model_json(raw, FALLBACK)
    assert result == {
        "candidate_email": "me@cv.com",
        "candidate_phone": "+61 400 000 000",
        "candidate_name": "Ada Lovelace",
        "years_experience": 9,
        "skills_match_score": 88,
        "red_flags": ["Job hopping"],
        "two_sentence_summary": "Strong. Hire.",
    }


def test_parse_model_json_uses_fallback_email_when_missing():
    raw = '{"candidate_name": "No Email", "skills_match_score": 50}'
    result = cva.parse_model_json(raw, FALLBACK)
    assert result["candidate_email"] == FALLBACK
    assert result["candidate_name"] == "No Email"
    assert result["candidate_phone"] == ""
    assert result["years_experience"] == 0
    assert result["red_flags"] == []


def test_parse_model_json_coerces_messy_types():
    raw = '{"years_experience": "5 years", "skills_match_score": "72/100", "red_flags": "Only one"}'
    result = cva.parse_model_json(raw, FALLBACK)
    assert result["years_experience"] == 5
    assert result["skills_match_score"] == 72
    assert result["red_flags"] == ["Only one"]


def test_parse_model_json_empty_raises():
    with pytest.raises(cva.ProviderError):
        cva.parse_model_json("", FALLBACK)


def test_parse_model_json_no_object_raises():
    with pytest.raises(cva.ProviderError):
        cva.parse_model_json("the model refused", FALLBACK)


# ---------------------------------------------------------------------------
# render_ack_body
# ---------------------------------------------------------------------------
def test_render_ack_body_substitutes_name():
    assert cva.render_ack_body("Hi [first_name],", "Sam") == "Hi Sam,"


def test_render_ack_body_defaults_to_there():
    assert cva.render_ack_body("Hi [first_name],", "") == "Hi there,"
    assert cva.render_ack_body("Hi [first_name],", "   ") == "Hi there,"


# ---------------------------------------------------------------------------
# parse_args (CLI)
# ---------------------------------------------------------------------------
def test_parse_args_defaults():
    args = cva.parse_args([])
    assert args.provider is None
    assert args.limit is None
    assert args.since is None
    assert args.dry_run is False


def test_parse_args_overrides():
    args = cva.parse_args(
        ["--provider", "anthropic", "--limit", "10", "--since", "01-Jan-2026", "--dry-run"]
    )
    assert args.provider == "anthropic"
    assert args.limit == 10
    assert args.since == "01-Jan-2026"
    assert args.dry_run is True


def test_parse_args_rejects_unknown_provider():
    with pytest.raises(SystemExit):
        cva.parse_args(["--provider", "nope"])


def test_apply_overrides_flags_win_over_env(monkeypatch):
    monkeypatch.setenv("PROVIDER", "bedrock")
    monkeypatch.setenv("IMAP_HOST", "imap.example.com")
    monkeypatch.setenv("IMAP_USER", "u")
    monkeypatch.setenv("IMAP_PASSWORD", "p")
    cfg = cva.Config()
    assert cfg.provider == "bedrock"
    cfg.apply_overrides(cva.parse_args(["--provider", "gateway", "--limit", "3", "--dry-run"]))
    assert cfg.provider == "gateway"
    assert cfg.max_emails == 3
    assert cfg.dry_run is True
