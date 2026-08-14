"""Parser tests: delimiters, checksum vector, transact time (doc 01 §1)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fixhelpers import build_fix, new_order

from fix42cache.parser import (
    PIPE,
    SOH,
    checksum_ok,
    parse_fix,
    parse_transact_time,
    render_fields,
    render_pipe,
)

# A frozen wire vector: FIX 4.2 new-order-single with hand-verified framing.
VECTOR_SOH = (
    "8=FIX.4.2\x019=97\x0135=D\x0134=2\x0149=OMS\x0156=VENUE\x01"
    "52=20240115-14:30:00.000\x0111=C1\x0155=IBM\x0154=1\x0138=1000\x01"
    "40=2\x0144=185.50\x0159=0\x0110=165\x01"
)


def test_parse_fix_soh_delimited() -> None:
    fields = parse_fix(VECTOR_SOH)
    assert fields[35] == "D"
    assert fields[11] == "C1"
    assert fields[55] == "IBM"
    assert fields[44] == "185.50"


def test_parse_fix_pipe_delimited() -> None:
    assert parse_fix(render_pipe(VECTOR_SOH)) == parse_fix(VECTOR_SOH)


def test_parse_fix_splits_on_first_equals_only() -> None:
    fields = parse_fix("35=8|58=price=185.50 rejected|17=E1")
    assert fields[58] == "price=185.50 rejected"
    assert fields[17] == "E1"


def test_parse_fix_skips_empty_segments() -> None:
    fields = parse_fix("||35=D||11=C1|||")
    assert fields == {35: "D", 11: "C1"}


def test_parse_fix_ignores_non_numeric_and_unterminated_segments() -> None:
    fields = parse_fix("35=D|junk|BAD=1|11=C1")
    assert fields == {35: "D", 11: "C1"}


def test_parse_fix_empty_input_returns_empty_dict() -> None:
    assert parse_fix("") == {}
    assert parse_fix("|||") == {}


def test_parse_fix_duplicate_tag_last_wins() -> None:
    assert parse_fix("35=8|17=E1|17=E2")[17] == "E2"


def test_parse_fix_keeps_pipes_inside_text_when_soh_delimited() -> None:
    raw = "35=8\x0158=a|b|c\x0117=E1\x01"
    assert parse_fix(raw)[58] == "a|b|c"


def test_parse_fix_tolerates_trailing_newline() -> None:
    assert parse_fix("35=D|11=C1|\n")[11] == "C1"


def test_render_pipe_replaces_soh() -> None:
    rendered = render_pipe(VECTOR_SOH)
    assert SOH not in rendered
    assert rendered.startswith("8=FIX.4.2|9=97|35=D|")


def test_render_fields_round_trips_through_parse_fix() -> None:
    fields = parse_fix(VECTOR_SOH)
    assert parse_fix(render_fields(fields)) == fields


def test_checksum_ok_true_for_valid_vector() -> None:
    assert checksum_ok(VECTOR_SOH) is True


def test_checksum_ok_matches_independent_computation() -> None:
    body_end = VECTOR_SOH.rfind("\x0110=") + 1
    expected = sum(VECTOR_SOH[:body_end].encode()) % 256
    assert expected == 165  # the vector's declared 10=165
    assert checksum_ok(VECTOR_SOH) is True


def test_checksum_ok_false_for_corrupted_checksum() -> None:
    corrupted = VECTOR_SOH.replace("10=165", "10=101")
    assert checksum_ok(corrupted) is False


def test_checksum_ok_false_when_body_tampered() -> None:
    tampered = VECTOR_SOH.replace("55=IBM", "55=MSF")
    assert checksum_ok(tampered) is False


def test_checksum_ok_none_when_no_tag_10() -> None:
    assert checksum_ok("8=FIX.4.2\x0135=D\x0111=C1\x01") is None
    assert checksum_ok("") is None


def test_checksum_ok_false_for_non_numeric_checksum() -> None:
    assert checksum_ok(VECTOR_SOH.replace("10=165", "10=abc")) is False


def test_checksum_ok_accepts_pipe_rendered_message() -> None:
    """A fixture keeps its checksum when rendered with '|' (doc 01 §1)."""
    assert checksum_ok(render_pipe(VECTOR_SOH)) is True


def test_builder_emits_valid_framing() -> None:
    raw = new_order("C1", delimiter=SOH)
    assert checksum_ok(raw) is True
    fields = parse_fix(raw)
    body_start = raw.index("35=")
    body_end = raw.rfind(SOH + "10=") + 1
    assert fields[9] == str(len(raw[body_start:body_end]))


@pytest.mark.parametrize("delimiter", [SOH, PIPE])
def test_builder_round_trips_in_both_delimiters(delimiter: str) -> None:
    raw = build_fix("D", {11: "C1", 38: 1000}, delimiter=delimiter)
    assert parse_fix(raw)[11] == "C1"
    assert checksum_ok(raw) is True


def test_parse_transact_time_with_millis() -> None:
    parsed = parse_transact_time("20240115-14:30:00.123")
    assert parsed == datetime(2024, 1, 15, 14, 30, 0, 123000, tzinfo=timezone.utc)
    assert parsed is not None and parsed.tzinfo is timezone.utc


def test_parse_transact_time_without_millis() -> None:
    assert parse_transact_time("20240115-14:30:05") == datetime(
        2024, 1, 15, 14, 30, 5, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("value", ["", None, "not-a-time", "2024-01-15T14:30:00Z"])
def test_parse_transact_time_invalid_returns_none(value: str | None) -> None:
    assert parse_transact_time(value) is None
