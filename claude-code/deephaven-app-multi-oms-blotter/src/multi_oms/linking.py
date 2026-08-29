"""Cross-hub linking primitives -- doc 09 section 4.

The only state this module adds to the system: a per-hub **sticky link map**
``{OrderKey: ExtOrdID}``.  The first non-empty link-tag value seen for a chain
wins; later conflicting values are ignored.

Why sticky rather than last-write-wins: a drop-copy link does not legitimately
change (the downstream order was routed from exactly one upstream order), while a
cancel/replace on the downstream side re-sends the whole order with the *same*
external id -- and an amend that omits the tag must not un-link a chain.  A tape
that does contradict itself is a data bug, and pinning the first value keeps the
family stable instead of making it flap.  Documented, not configurable (doc 09
section 4 step 4).

Pure stdlib: no Deephaven, no ``fix42cache`` -- so all of it is unit-tested on a
bare host python.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

__all__ = [
    "GLOBAL_KEY_SEPARATOR",
    "LinkTracker",
    "global_key",
    "sanitize_hub",
    "augment_row",
    "augment_state_row",
    "augment_hub_row",
]

#: Separator between the hub name and the hub-local ``OrderKey``.
#:
#: ``|`` never appears in a FIX identifier the generator produces, and the joined
#: value is only ever compared for equality, so the key stays unambiguous.
GLOBAL_KEY_SEPARATOR = "|"

#: Characters allowed in a sanitized hub name (Deephaven column-name safe).
_ALLOWED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"
)


def global_key(oms: Any, order_key: Any) -> str:
    """Build the cross-hub primary key ``"<Oms>|<OrderKey>"``.

    Args:
        oms: Hub name.
        order_key: The hub-local ``OrderKey`` (doc 01 section 3).

    Returns:
        The composite key. ``None`` on either side degrades to ``""`` so a row can
        always be published (a blank key is visible in the blotter; a ``None``
        would poison the batch).
    """
    left = "" if oms is None else str(oms)
    right = "" if order_key is None else str(order_key)
    return left + GLOBAL_KEY_SEPARATOR + right


def sanitize_hub(name: Any) -> str:
    """Reduce a hub name to ``[A-Za-z0-9_]`` for use as a column-name suffix.

    ``"OMS-B-parent"`` becomes ``"OMS_B_parent"``, which is what makes
    ``CumQty_OMS_B_parent`` (doc 09 section 5.5's ``chain_recon`` pivot) a legal
    Deephaven column name.

    Args:
        name: The configured hub name.

    Returns:
        The sanitized name; every disallowed character becomes ``_``. An empty or
        all-illegal name yields ``"_"``, and a leading digit is prefixed with ``_``
        so the result is always a legal identifier.
    """
    text = "" if name is None else str(name)
    cleaned = "".join(ch if ch in _ALLOWED else "_" for ch in text)
    if not cleaned:
        return "_"
    if cleaned[0].isdigit():
        return "_" + cleaned
    return cleaned


class LinkTracker:
    """Per-hub sticky ``{OrderKey: ExtOrdID}`` map.

    Bounded by the number of orders on the hub's tape, single-writer (one hub
    listener owns one tracker), and pure python -- the whole reason the linking
    rule is testable without a Deephaven server.
    """

    __slots__ = ("_links", "_conflicts")

    def __init__(self) -> None:
        """Start with an empty map."""
        self._links: Dict[str, str] = {}
        self._conflicts = 0

    def record(self, order_key: Any, ext_ord_id: Any) -> str:
        """Bind ``order_key`` to its first non-empty external id and return it.

        Args:
            order_key: The hub-local ``OrderKey``.
            ext_ord_id: The link-tag value from the current message; ``""``/``None``
                when this message did not carry the tag.

        Returns:
            The sticky external id for ``order_key`` -- the newly recorded value,
            the previously recorded one, or ``""`` when none has ever been seen.
        """
        key = "" if order_key is None else str(order_key)
        if not key:
            return ""
        value = "" if ext_ord_id is None else str(ext_ord_id).strip()
        existing = self._links.get(key)
        if existing:
            if value and value != existing:
                self._conflicts += 1
            return existing
        if value:
            self._links[key] = value
            return value
        return ""

    def get(self, order_key: Any) -> str:
        """Return the sticky external id for ``order_key`` (``""`` if unlinked)."""
        key = "" if order_key is None else str(order_key)
        return self._links.get(key, "")

    @property
    def conflicts(self) -> int:
        """How many later, contradicting link values were ignored."""
        return self._conflicts

    @property
    def links(self) -> Mapping[str, str]:
        """A read-only view of the sticky map (for banners and tests)."""
        return dict(self._links)

    def __len__(self) -> int:
        """Number of linked order chains."""
        return len(self._links)

    def __contains__(self, order_key: object) -> bool:
        """True when ``order_key`` has a recorded external id."""
        return bool(self.get(order_key))


# --------------------------------------------------------------------------------------
# Row augmentation -- doc 09 section 4.1 (added columns lead, before OrderKey)
# --------------------------------------------------------------------------------------


def augment_row(
    row: Mapping[str, Any],
    oms: str,
    ext_ord_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Copy a ``fix42cache`` row dict with ``Oms``/``GlobalKey`` (and ``ExtOrdID``) leading.

    Args:
        row: A ``to_row()`` dict from ``fix42cache`` (``OrderState``,
            ``ExecutionRow``, ``OrderEventRow``).
        oms: The hub name.
        ext_ord_id: The sticky external id; pass ``None`` for streams that do not
            carry one (executions, events), ``""`` for an unlinked order.

    Returns:
        A new dict. The added keys are written first and are authoritative -- a
        source row that happened to carry the same key cannot overwrite them.
    """
    out: Dict[str, Any] = {
        "Oms": oms,
        "GlobalKey": global_key(oms, row.get("OrderKey", "")),
    }
    if ext_ord_id is not None:
        out["ExtOrdID"] = ext_ord_id
    for key, value in row.items():
        if key not in out:
            out[key] = value
    return out


def augment_state_row(row: Mapping[str, Any], oms: str, ext_ord_id: str) -> Dict[str, Any]:
    """``augment_row`` for ``oms_order_state_blink`` -- always carries ``ExtOrdID``."""
    return augment_row(row, oms, ext_ord_id="" if ext_ord_id is None else ext_ord_id)


def augment_hub_row(row: Mapping[str, Any], oms: str) -> Dict[str, Any]:
    """Copy a row dict with only ``Oms`` leading.

    Used for ``oms_fix_messages_blink`` and ``oms_ingest_errors``, which are per-hub
    audit tapes and are not keyed by ``GlobalKey`` (doc 09 section 4.1).
    """
    out: Dict[str, Any] = {"Oms": oms}
    for key, value in row.items():
        if key not in out:
            out[key] = value
    return out
