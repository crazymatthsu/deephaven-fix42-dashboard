"""Scope URIs, Barrage tickets and global-name sanitising -- doc 10 sections 3 and 6.

Pure stdlib: a leaf URI is validated (and its host/port extracted) at collector
*startup*, before a single gRPC call is made, so a typo in ``REMOTEURI_LEAVES``
produces one actionable message instead of a resolve timeout five minutes later.

Two address forms are in play and they are not interchangeable:

``dh+plain://dh1:10000/scope/rx_orders``
    the **URI** ``deephaven.uri.resolve`` takes -- scheme, authority, then the
    ``/scope/<global>`` path naming a global in the remote server's script session.
``b"s/rx_orders"``
    the **ticket** ``barrage_session().subscribe`` / ``.snapshot`` take -- the same
    global, addressed by its scope ticket. ``s/`` is Deephaven's scope-ticket
    prefix; a global bound by a console ``executeCode`` is reachable through it
    exactly like an app-mode global (doc 10 section 3).
"""

from __future__ import annotations

from typing import Tuple

from multi_oms.linking import sanitize_hub

__all__ = [
    "DEFAULT_PORT",
    "URI_SCHEMES",
    "SCOPE_PATH",
    "TICKET_PREFIX",
    "LEAF_EXPORTS",
    "normalize_leaf_uri",
    "scope_uri",
    "scope_ticket",
    "host_port",
    "global_suffix",
    "leaf_global_name",
    "raw_global_name",
]

#: Port assumed when a leaf URI carries only a host (doc 10 section 4.3).
DEFAULT_PORT = 10000

#: Accepted leaf URI schemes: plaintext (the stack's anonymous-auth default) and TLS.
URI_SCHEMES: Tuple[str, ...] = ("dh+plain://", "dh://")

#: Path segment addressing a global in the remote script session.
SCOPE_PATH = "/scope/"

#: Barrage scope-ticket prefix.
TICKET_PREFIX = "s/"

#: The four globals every leaf must expose before the collector can build its DAG
#: (doc 10 sections 5.3 and 6). "healthy" is not "exported": the gRPC probe passes
#: long before Application Mode finishes wiring, so the collector waits for these.
LEAF_EXPORTS: Tuple[str, ...] = ("rx_orders", "rx_id_index", "rx_exposure", "rx_leaf_stats")


def normalize_leaf_uri(uri: object, label: str = "uri") -> str:
    """Validate a leaf URI and return it without a trailing slash.

    Args:
        uri: The configured URI, e.g. ``"dh+plain://dh1:10000"``.
        label: Name used in the error message (the offending JSON key or env var).

    Returns:
        The URI with any trailing ``/`` removed, so :func:`scope_uri` can append
        ``/scope/<name>`` unconditionally.

    Raises:
        ValueError: If the scheme is not one of :data:`URI_SCHEMES`, the authority
            is empty, a port is present but not a positive integer, or a path other
            than ``/`` is present -- the leaf URI addresses a *server*, and the
            ``/scope/...`` part is this module's to append.
    """
    text = ("" if uri is None else str(uri)).strip()
    if not text:
        raise ValueError(f"{label} is empty; expected e.g. 'dh+plain://dh1:10000'")
    for scheme in URI_SCHEMES:
        if text.startswith(scheme):
            rest = text[len(scheme) :]
            break
    else:
        raise ValueError(
            f"{label}={text!r} must start with one of {list(URI_SCHEMES)}; "
            "'dh+plain://' is the stack's anonymous-auth default, 'dh://' is TLS"
        )
    rest = rest.rstrip("/")
    if "/" in rest:
        raise ValueError(
            f"{label}={text!r} must name a server, not a table: drop the "
            f"{SCOPE_PATH.strip('/')!r} path -- the collector appends it per export"
        )
    if not rest:
        raise ValueError(f"{label}={text!r} has no host; expected e.g. 'dh+plain://dh1:10000'")
    # Parse for the side effect of validating the port.
    host_port(text, label=label)
    return text.rstrip("/")


def scope_uri(leaf_uri: str, name: str) -> str:
    """Build the ``deephaven.uri.resolve`` URI of one global on one leaf.

    Args:
        leaf_uri: A leaf's server URI (``"dh+plain://dh1:10000"``).
        name: The global's name (``"rx_orders"``).

    Returns:
        ``"dh+plain://dh1:10000/scope/rx_orders"``.
    """
    return f"{str(leaf_uri).rstrip('/')}{SCOPE_PATH}{name}"


def scope_ticket(name: str) -> bytes:
    """Build the Barrage scope ticket for a global (``b"s/rx_orders"``).

    ``barrage_session().subscribe`` / ``.snapshot`` take **bytes**, not a string --
    a str argument fails inside the Java client with a much less obvious message.
    """
    return (TICKET_PREFIX + str(name)).encode("utf-8")


def host_port(uri: str, label: str = "uri") -> Tuple[str, int]:
    """Split a leaf URI into ``(host, port)`` for ``barrage_session``.

    Args:
        uri: A leaf's server URI.
        label: Name used in the error message.

    Returns:
        ``("dh1", 10000)``; the port defaults to :data:`DEFAULT_PORT` when absent.

    Raises:
        ValueError: On an unknown scheme or an unparseable/out-of-range port.
            IPv6 literals are not supported (and are not used by the compose
            network); a bracketed host is rejected here rather than mis-split.
    """
    text = ("" if uri is None else str(uri)).strip()
    for scheme in URI_SCHEMES:
        if text.startswith(scheme):
            rest = text[len(scheme) :]
            break
    else:
        raise ValueError(f"{label}={text!r} must start with one of {list(URI_SCHEMES)}")
    rest = rest.split("/", 1)[0]
    if rest.startswith("["):
        raise ValueError(
            f"{label}={text!r} looks like an IPv6 literal, which is not supported; "
            "use a host name (the compose network provides one)"
        )
    host, sep, port_text = rest.partition(":")
    if not host:
        raise ValueError(f"{label}={text!r} has no host; expected e.g. 'dh+plain://dh1:10000'")
    if not sep:
        return host, DEFAULT_PORT
    try:
        port = int(port_text)
    except ValueError:
        raise ValueError(
            f"{label}={text!r} has a non-numeric port {port_text!r}; expected e.g. 10000"
        ) from None
    if not 1 <= port <= 65535:
        raise ValueError(f"{label}={text!r} has port {port}, which is out of range 1-65535")
    return host, port


def global_suffix(leaf_name: object) -> str:
    """Reduce a leaf name to a lower-case python-identifier suffix.

    ``"DH1"`` -> ``"dh1"``, ``"eu-west 2"`` -> ``"eu_west_2"``: this is what makes
    ``rx_orders_dh1`` a legal global whatever the topology calls its leaves.
    """
    return sanitize_hub(leaf_name).lower()


def leaf_global_name(base: str, leaf_name: object) -> str:
    """Name of one leaf's copy of an export (``"rx_orders"``, ``"DH1"`` -> ``rx_orders_dh1``)."""
    return f"{base}_{global_suffix(leaf_name)}"


def raw_global_name(hub_name: object) -> str:
    """Global under which one hub's raw AMPS blink table is exported.

    ``"OMS-B-parent"`` -> ``"oms_raw_oms_b_parent"``: doc 09's raw-global naming,
    reused verbatim so a leaf's panels look like the single-server blotter's.
    """
    return "oms_raw_" + global_suffix(hub_name)
