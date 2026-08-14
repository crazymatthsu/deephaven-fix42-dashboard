"""Lenient FIX 4.2 wire-format helpers (doc 01 §1).

The parser never rejects a message: framing on audit/drop-copy feeds is
routinely stripped or rewritten, so validation results are *recorded*
(``ChecksumOk``) rather than enforced.
"""

from __future__ import annotations

from datetime import datetime, timezone

__all__ = [
    "SOH",
    "PIPE",
    "parse_fix",
    "render_pipe",
    "render_fields",
    "checksum_ok",
    "parse_transact_time",
]

#: The real FIX field delimiter.
SOH = "\x01"
#: Display/fixture delimiter accepted by the parser.
PIPE = "|"

_CHECKSUM_MARKER = "10="
_TIME_FORMATS = ("%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S")


def parse_fix(raw: str) -> dict[int, str]:
    """Parse a raw FIX 4.2 message into ``{tag: value}``.

    * Accepts either the SOH (``\\x01``) delimiter or ``|`` (fixtures/tests).
      If any SOH is present the message is split on SOH only, so a ``|``
      inside a free-text field is preserved.
    * Each field is split on its **first** ``=`` only (values may contain ``=``).
    * Empty segments and segments whose tag is not numeric are skipped.
    * Repeated tags: the last occurrence wins (this project has no repeating
      groups in scope).
    """
    fields: dict[int, str] = {}
    if not raw:
        return fields
    text = raw.rstrip("\r\n")
    segments = text.split(SOH) if SOH in text else text.split(PIPE)
    for segment in segments:
        if not segment:
            continue
        tag_text, sep, value = segment.partition("=")
        if not sep:
            continue
        tag_text = tag_text.strip()
        if not tag_text.isdigit():
            continue
        fields[int(tag_text)] = value
    return fields


def render_pipe(raw: str) -> str:
    """Render a raw message for display: SOH delimiters become ``|``."""
    return raw.replace(SOH, PIPE)


def render_fields(fields: dict[int, str]) -> str:
    """Render ``{tag: value}`` back to a pipe-delimited string (audit display).

    Used when the state machine is fed pre-parsed fields and therefore has no
    original wire string to publish.  Field order follows the dict order; no
    ``9``/``10`` framing is recomputed.
    """
    return PIPE.join(f"{tag}={value}" for tag, value in fields.items())


def checksum_ok(raw: str) -> bool | None:
    """Validate tag 10 CheckSum.

    Returns ``True``/``False``, or ``None`` when the message carries no tag 10.

    The checksum is ``sum(bytes) % 256`` over everything up to **and
    including** the SOH preceding ``10=``.  Pipe-delimited fixtures are
    normalised back to SOH before summing, so a message checksums identically
    in either delimiter form.
    """
    if not raw:
        return None
    text = raw.rstrip("\r\n")
    if SOH not in text and PIPE in text:
        text = text.replace(PIPE, SOH)

    marker_at = text.rfind(SOH + _CHECKSUM_MARKER)
    if marker_at >= 0:
        body_end = marker_at + 1  # include the SOH itself
        value_at = marker_at + 1 + len(_CHECKSUM_MARKER)
    elif text.startswith(_CHECKSUM_MARKER):
        body_end = 0
        value_at = len(_CHECKSUM_MARKER)
    else:
        return None

    value_end = text.find(SOH, value_at)
    declared = (text[value_at:] if value_end < 0 else text[value_at:value_end]).strip()
    if not declared.isdigit():
        return False
    computed = sum(text[:body_end].encode("utf-8")) % 256
    return computed == int(declared)


def parse_transact_time(value: str | None) -> datetime | None:
    """Parse ``YYYYMMDD-HH:MM:SS`` with optional ``.sss`` into a UTC datetime.

    Returns ``None`` for missing/empty/unparseable values.  Used for both tag 60
    TransactTime and tag 52 SendingTime.
    """
    if not value:
        return None
    text = value.strip()
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None
