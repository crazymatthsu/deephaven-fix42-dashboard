# FIX 4.2 Identity, Linking, and Apply Rules

Drop-copy does not give one stable primary key. It gives a **family of
identifiers** that appear, change, and chain. The engine's first job is
to collapse that family onto one `OrderKey`.

## Identifier tags

| Tag | Name | Who assigns it | Lifetime |
|-----|------|----------------|----------|
| 11 | `ClOrdID` | Client / OMS | Changes on every cancel/replace |
| 41 | `OrigClOrdID` | Client / OMS | Previous `ClOrdID` (not the day's first) |
| 37 | `OrderID` | Broker / venue | Stable across replaces |
| 198 | `SecondaryOrderID` | Broker / venue | Optional second broker id |
| 17 | `ExecID` | Broker / venue | Unique per execution report |
| 19 | `ExecRefID` | Broker / venue | `ExecID` being busted or corrected |

`ClOrdID` is required on electronically submitted orders. `OrderID` is
required on Execution Report and is the only identifier that is **not**
required to change when the client amends.

`ExecID` identifies an **event**, never an order. The engine indexes
`ExecID → OrderKey` so `getByExecId` and `35=Q` work.

## Resolution order

When a message arrives:

1. If `OrderID (37)` is present **and** already mapped → that `OrderKey`.
2. Else if `ClOrdID (11)` is present **and** already mapped → that key.
3. Else if `OrigClOrdID (41)` is present **and** already mapped → that key.
4. Else if `ExecID (17)` / `ExecRefID (19)` is mapped → that key.
5. Else create a new order:
   - `OrderKey = OrderID` if present, otherwise `ClOrdID`.
6. Bind every identifier present on the message to that `OrderKey`.

Preferring `OrderID` heals a broken `ClOrdID`/`OrigClOrdID` chain as soon
as the next Execution Report arrives. Replay of the same tape must
converge on the same keys.

### Why `OrderID` arrives late

A `D` has only `ClOrdID`. `OrderID` first appears on the first `8`.
Until then the order is keyed by `ClOrdID`. The first ER **rekeys**
`OrderKey` from `C1` to `O9` and rebinds every alias. Downstream
Deephaven tables see a `PreviousOrderKey` column so `last_by` on the
old key can be dropped (the engine also emits a tombstone row).

## Cancel / replace chaining

FIX 4.2 requires:

- On `F` and `G`, `OrigClOrdID` is the **current** `ClOrdID` of the
  working order.
- The request's own `ClOrdID` is a **new** client id.
- Resulting ER: `150=E` / `150=6` echo the new id; `150=5` makes the
  new `ClOrdID` current; `150=4` is terminal.
- `9` carries `CxlRejResponseTo (434)`: `1` = reject of `F`, `2` =
  reject of `G`, plus the last accepted `OrdStatus (39)`.

The engine:

- On `G`/`F`: attach the **new** `ClOrdID` immediately (so later ERs
  that only carry the new id still join).
- Set pending status locally (`E` / `6`) unless the order is already
  terminal.
- On `8` with `150=5` or `150=4`: take the ER as authoritative.
- On `9`: restore `OrdStatus` from tag 39 and clear the pending flag
  indicated by `434`.

## Parent / child

FIX 4.2 has no standard parent-order tag. Default user-defined tags:

| Tag | Name | Meaning |
|-----|------|---------|
| 20001 | `ParentOrderID` | Broker id of the parent |
| 20002 | `ParentClOrdID` | Client id of the parent |

Parent and child are independent rows. Children never overwrite the
parent's `CumQty` / `OrdStatus`. Rollup (sum of child qty) is computed
on read / as a Deephaven `agg_by`, not stored on the parent.

## Out-of-order and duplicates

- **Duplicates:** same `ExecID` + same `ExecTransType` is a no-op for
  quantity fields; identifiers are still rebound.
- **Stale ERs:** if `TransactTime (60)` is present and older than the
  last **venue** `TransactTime` (updated only by `35=8`), do not
  overwrite status/qty. Identifiers still bind. Client `D`/`G`/`F`
  times are not the watermark — they can sit ahead of the venue clock.
  Configurable (`applyStaleExecReports`, default false).
- **Missing New:** an ER or cancel with no prior `D` still creates the
  order. Audit streams often start mid-day.

## Per-message apply

Blank incoming fields never wipe a populated field.

### `35=D` New Order - Single

Create if unknown; if known (replay), refresh instruction fields.
`OrdStatus` becomes `A` (Pending New) if no ER has arrived.
`LeavesQty = OrderQty` if no ER yet.

### `35=8` Execution Report

Authoritative for venue state, subject to stale-`TransactTime`.

- Bind `OrderID`, `ClOrdID`, `OrigClOrdID`, `ExecID`.
- Apply `OrdStatus`, `ExecType`, `ExecTransType`.
- Apply `CumQty`, `LeavesQty`, `LastShares`/`LastQty`, `LastPx`,
  `AvgPx`, restated `OrderQty`.
- `ExecTransType=1` (bust) / `2` (correct): trust the **restated**
  `CumQty`/`LeavesQty` on the ER. Do not invent a reversal.
- `ExecTransType=3` (Status): apply status/qty; do not treat as a new
  fill for fill de-dup (the `ExecID` is still recorded).
- Clear pending flags when status is no longer `6`/`E`, or when
  terminal (`2` Filled, `4` Canceled, `8` Rejected, `C` Expired).

FIX 4.2 has **no fills repeating group**. Each fill is its own `8`
with `150=1` or `150=2`.

### `35=G` / `35=F`

Resolve via `OrigClOrdID` / `OrderID` / `ClOrdID`. Bind the new
`ClOrdID`. Set `pending_replace` / `pending_cancel`. Requested qty /
price are written onto the same fields and confirmed by the ER.

### `35=9` Order Cancel Reject

Apply `OrdStatus (39)`. Record `CxlRejReason`, `CxlRejResponseTo`,
`Text`. Clear the pending flag indicated by `434`.

### `35=Q` Don't Know Trade

Set `dk_trade=true`, record `DKReason` and `Text`. **Do not** unwind
`CumQty`. A DK is a claim; the bust arrives later as `20=1`.

## ExecType / OrdStatus (FIX 4.2)

`150 ExecType` is why the report exists. `39 OrdStatus` is the resulting
order state.

| Code | ExecType | Code | OrdStatus |
|------|----------|------|-----------|
| 0 | New | 0 | New |
| 1 | Partial fill | 1 | Partially filled |
| 2 | Fill | 2 | Filled (terminal) |
| 4 | Canceled | 4 | Canceled (terminal) |
| 5 | Replace | 5 | Replaced |
| 6 | Pending cancel | 6 | Pending cancel |
| 8 | Rejected | 8 | Rejected (terminal) |
| A | Pending new | A | Pending new |
| C | Expired | C | Expired (terminal) |
| E | Pending replace | E | Pending replace |
| D | Restated | | |

`20 ExecTransType`: `0` New, `1` Cancel (bust), `2` Correct, `3` Status.

## Worked chain

```
D  11=C1                  → key=C1, status=A
8  11=C1 37=O9 17=E1 150=0 → key=O9 (rekey), status=0
8  11=C1 37=O9 17=E2 150=1 32=400 → partial, cum=400
G  11=C2 41=C1 37=O9      → C2 bound, pending_replace
8  11=C2 41=C1 37=O9 150=5 → C2 current, status back to 1
9  (if G rejected)        → status from tag 39, pending cleared
Q  17=E2                  → dk_trade=true, cum unchanged
```

`getByClOrdId("C1")`, `getByClOrdId("C2")`, `getByOrderId("O9")`,
`getByExecId("E1")` all return the same latest `OrderState`.
