# On-Demand Executions from AMPS — Design Idea (TABLED)

> **Status: tabled. This is not an agreed design and nothing here is implemented.**
>
> **Blocker: it creates two sources of truth for the same executions.** The memory win is also only
> partial. Cheaper answers to the same problem are in §4.
>
> Unlike docs 00–07, which are binding contracts for the implementation, this document is a record
> of an idea that was explored and set aside, kept so the reasoning is not re-derived later.

---

## 1. The idea

The Executions panel is a `where` filter over `executions`, a Deephaven append-only table holding
**every execution ever seen** ([`dag.py`](../deephaven-scripts/src/dh_app/dag.py) §`build_derived`).
Memory grows without bound with order flow.

If all executions were stored in AMPS, clicking an order could fetch just that order's executions on
demand, and Deephaven would stop holding the history.

## 2. The shape that was designed

Constraints taken as given: the AMPS query runs in the existing `:amps-connectors` app, the panel
must stay **live** (not a snapshot), and scope is the executions history only.

```
  Deephaven                              amps-connectors                     AMPS
  ─────────                              ───────────────                     ────
  dashboard click
    └─ PUT /views/{viewId} {orderId} ──►  ExecutionQueryManager
                                            └─ ExecutionQuery ─ sow_and_subscribe ──►
                                                 (filter /OrderID = '…')
  executions_live  ◄── Flight addToInputTable ──┘  decode → map → batch
  (keyed ViewId,ExecID)
```

- **Control plane over HTTP.** `PUT /api/executions/views/{viewId} {"orderId": …}` — declarative
  and idempotent: *"this panel now shows this order"*. Collapses unsubscribe-old plus
  subscribe-new into one call, so a lost call cannot leak a subscription. `DELETE` on unmount.
- **Data plane over Arrow Flight.** Liveness rules out HTTP request/response for the rows, so the
  connector's existing `FlightDeephavenGateway.addRows` / `deleteRows` carries them.
- **AMPS side.** One `sow_and_subscribe` per view, filtered to the order: the SOW replay supplies
  the history, the subscription streams new fills.
- **Deephaven side.** One shared `executions_live` keyed on `(ViewId, ExecID)`. Each panel mints a
  `view_id` and filters on it, so two browser sessions write disjoint key ranges and read disjoint
  filters rather than fighting over one table.

## 3. Why it is tabled

### 3.1 Two sources of truth

The panel would read **AMPS** (raw venue FIX) while `executions_latest` and `execid_index` keep
reading **`fix42cache`** (normalized enum names, plus `IsFill`, `FillStatus` and `OrderKey`, which
the state machine computes and the wire never carries).

That is two independent derivations of the same executions, visible in the same session, free to
disagree:

- **Cosmetically** — if AMPS carries `150=2` where `fix42cache` produces `FILL`, the panel regresses
  visibly and contradicts `executions_latest` on screen.
- **Silently** — if the AMPS SOW's bust/correct handling differs from the transition rules in
  [doc 01 §6](01-fix42-messages-and-state-machine.md), the disagreement is semantic and no one
  notices.
- The three computed columns cannot come from AMPS at all, so they would need re-deriving
  Deephaven-side — a **third** derivation of the same facts.

The scope decision that keeps `executions_latest` is what forces the duplication: that node is
precisely the one that would otherwise have retired alongside `executions`.

### 3.2 The memory win is partial

`executions_latest = executions_blink.last_by("ExecID")` retains one row per distinct `ExecID`
**forever**, in the same 19 columns ([doc 02 §1.1](02-deephaven-table-types.md) — blink aggregation
retains per-key state). Every execution has a unique `ExecID`, so that table is *also*
O(total executions).

Dropping `executions` removes only the re-emission rows layered on top — one extra row per bust,
correct or DK. One O(E) table deleted, another O(E) table kept, and a second process added to the
interaction path to buy it.

## 4. Cheaper answers to the same problem

In the order worth trying:

1. **`ring_table` on the audit tables.** [Doc 02 §1.3](02-deephaven-table-types.md) already names
   this as "the production-hardening option for the audit tables if the feed is unbounded". Bounded
   memory, a one-line change, no second process, no divergence, no new failure modes. Almost
   certainly what to do instead.
2. **Target `fix_messages` first.** It is O(M) over **33 columns including the raw FIX string** — by
   some distance the largest table on the server, and nothing to do with executions. Ringing or
   dropping it is the cheapest real win available.
3. **Retire `executions_latest` too.** The only version of the AMPS idea that removes the
   duplication rather than creating it: AMPS becomes the *sole* source of executions in Deephaven,
   and `get_by_execid` is served from it as well. Bigger change, but one source of truth.
4. **Publish normalized executions to AMPS from `fix42cache`.** If AMPS held exactly the rows the
   state machine produces — same schema, `IsFill` / `FillStatus` / `OrderKey` included — fetched rows
   would be identical to what Deephaven would have held and the divergence disappears. Makes AMPS
   downstream of the pipeline rather than an independent upstream, and the connector bidirectional.

## 5. What would have to be true to revive this

- **One derivation of executions, not two** — i.e. option 3 or 4 above.
- **The AMPS `Executions` topic validated against a real broker.** Topic name, SOW key, filter
  syntax and field encodings were all unverifiable from this repo — there is no AMPS server, no
  topic definition and no sample payload here, so the entire config model was a template.
- **A decision on tag 37 changing on a replace.** FIX 4.2 permits it; a `/OrderID = '<current>'`
  filter would then miss pre-amend fills.

## 6. Findings worth keeping regardless

These came out of the exploration and hold whether or not the idea is ever revived:

- **`OrderKey` is synthetic.** `key = order_id or clordid or orig_clordid`
  ([`state_machine.py`](../deephaven-scripts/src/fix42cache/state_machine.py) `_resolve`), so AMPS
  has never seen it. Any AMPS query would have to filter on `OrderID` (tag 37), which
  `ORDER_GRID_LEAD_COLUMNS` already carries in the orders grid.
- **`executions_latest` and `execid_index` derive from `executions_blink`, not from `executions`.**
  The append-only node can therefore be removed without breaking either of them, or `get_by_execid`.
- ~~**`TableBootstrapScript.createIfMissing` compares column names and order only.**~~ *Fixed.*
  A dtype or `key_cols` mismatch used to pass silently and fail later at `addToInputTable`; the
  generated python now checks column types and keys as well as names and order
  ([doc 07 §8](07-amps-connectors.md)).
- **`FlightDeephavenGateway.deleteRows` returns early when the schema is not keyed.** Any on-demand
  table that needs row removal must be a keyed input table; an append-only one has no removal path
  and would grow exactly as much as the table being replaced.
- **Two docs assert an invariant this idea would break.**
  [Doc 03 §2.6](03-deephaven-dag.md) and the `dashboard.py` module docstring both state that
  click-through is purely `where` filters and that the DAG never changes shape at runtime. Any
  version of this idea makes that half true — the history panel still is, the executions panel
  would not be.
