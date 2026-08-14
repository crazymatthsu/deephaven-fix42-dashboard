# FIX 4.2 Messages, Linking & the Order State Machine (contract)

This is the **binding contract** for the generator (Java), the state-machine core
(`fix42cache`, Python), and the Deephaven DAG (`dh_app`). It condenses the domain
analysis from `fix42-oms-cache` (docs 00/01/06 there, including the corrections applied
after adversarial review) into exactly what this dashboard needs.

In scope: `35=D` NewOrderSingle, `35=G` OrderCancelReplaceRequest, `35=F`
OrderCancelRequest, `35=8` ExecutionReport, `35=9` OrderCancelReject, `35=Q` DontKnowTrade.
(The feed is an audit/drop-copy view: we see both client requests and venue responses.)

---

## 1. Wire format

- `tag=value` pairs delimited by SOH (`\x01`). The parser also accepts `|` as a
  delimiter for tests/fixtures. Split each field on the **first** `=` only.
- Header: `8=FIX.4.2`, `9=BodyLength`, `35=MsgType` first three, in order; also
  `49 SenderCompID`, `56 TargetCompID`, `34 MsgSeqNum`, `52 SendingTime`.
- `9 BodyLength` = byte count from the first char of `35=` through the SOH before `10=`.
- `10 CheckSum` = (sum of all bytes up to and including the SOH before `10=`) mod 256,
  zero-padded to 3 digits. The **generator must emit correct 9 and 10**; the **parser
  validates leniently** (records `ChecksumOk` boolean; never rejects the message —
  audit feeds sometimes strip framing).

## 2. Tag vocabulary used by this project

| Tag | Name | Notes |
|---|---|---|
| 11 | ClOrdID | new value on every D/G/F |
| 41 | OrigClOrdID | on G/F/9 (and echoed on replace/cancel 8s): the ClOrdID being acted on |
| 37 | OrderID | venue id, stable per chain; first appears on the first 8 |
| 17 | ExecID | unique per 8; dedupe key |
| 19 | ExecRefID | on bust/correct: the ExecID being cancelled/corrected |
| 20 | ExecTransType | 0=New 1=Cancel(bust) 2=Correct 3=Status |
| 150 | ExecType | 0 New, 1 PartialFill, 2 Fill, 3 DoneForDay, 4 Canceled, 5 Replaced, 6 PendingCancel, 8 Rejected, A PendingNew, E PendingReplace, D Restated, C Expired |
| 39 | OrdStatus | 0 New, 1 PartiallyFilled, 2 Filled, 3 DoneForDay, 4 Canceled, 5 Replaced, 6 PendingCancel, 8 Rejected, A PendingNew, C Expired, E PendingReplace |
| 1 | Account | index key |
| 55 | Symbol | index key |
| 54 | Side | 1 Buy, 2 Sell, 5 SellShort |
| 38 | OrderQty | |
| 40 | OrdType | 1 Market, 2 Limit |
| 44 | Price | required when 40=2 |
| 59 | TimeInForce | 0 Day, 1 GTC, 3 IOC, 4 FOK |
| 14 | CumQty | absolute snapshot on every 8 |
| 151 | LeavesQty | absolute snapshot on every 8 |
| 6 | AvgPx | absolute snapshot on every 8 |
| 32 | LastShares | this fill's qty (4.2 name; 4.4 renamed LastQty) |
| 31 | LastPx | this fill's price |
| 30 | LastMkt | execution market |
| 103 | OrdRejReason | on order reject 8s |
| 102 | CxlRejReason | on 9 |
| 434 | CxlRejResponseTo | on 9: 1=cancel(F) rejected, 2=replace(G) rejected |
| 127 | DKReason | on Q: A..F, Z |
| 58 | Text | free text |
| 60 | TransactTime | `YYYYMMDD-HH:MM:SS(.sss)` UTC |
| 21 | HandlInst | required on D/G in 4.2 (generator emits `1`) |

FIX 4.2 facts that shape the design:
- Each fill is its **own** ExecutionReport (`150=1`/`2` + tags 32/31). There is no fills
  repeating group in 4.2.
- Trade **bust** = `8` with `20=1`, `19=<busted ExecID>`; trade **correct** = `8` with
  `20=2`, `19=<corrected ExecID>` and new 32/31. In both, the venue restates 14/151/6/39
  as **absolute** values — the cache adopts them, it does **not** re-derive.
- `9 OrderCancelReject` may carry `37=NONE` when the target was never acked.

## 3. Identity resolution (chain keying)

Every order chain gets one stable **`OrderKey`**:

```
resolve(msg) -> OrderKey:
  1. if 37 OrderID present and mapped        -> its OrderKey
  2. elif 11 ClOrdID present and mapped      -> its OrderKey
  3. elif 41 OrigClOrdID present and mapped  -> its OrderKey
  4. else create chain:
        OrderKey = OrderID if present else ClOrdID
  finally: bind EVERY identifier present (37, 11, 41, 17) -> OrderKey (idempotent)
```

- Binding is idempotent; replays converge. OrderID is preferred because ClOrdID chains
  break if an intermediate replace is missed; OrderID heals the chain on the next 8.
- `ExecID` maps to the chain (`exec_index`) but is never itself an OrderKey.
- The state machine owns these dicts: `key_by_order_id`, `key_by_clordid`,
  `key_by_execid` — this is why **every row it publishes carries `OrderKey`**, letting
  the Deephaven DAG stay stateless.

## 4. OrderState — the cache value

One row per order chain (published as a snapshot after **every** message applied to it;
`last_by(OrderKey)` in Deephaven keeps the latest). Fields:

| Column | Type | Source |
|---|---|---|
| OrderKey | string | §3 |
| OrderID | string | 37 (empty until first 8) |
| ClOrdID | string | current client id (rotates on replace confirm) |
| OrigClOrdID | string | latest 41 seen |
| RootClOrdID | string | ClOrdID of the original D |
| ClOrdIDChain | string | comma-joined chain history, oldest→newest |
| Account, Symbol | string | from D (or first message carrying them) |
| Side, OrdType, TimeInForce | string | enum names (`BUY`, `LIMIT`, `DAY`, …) |
| OrderQty, Price, StopPx | double | current terms (see §6 G rules) |
| OrdStatus | string | enum name from tag 39 / machine (`PENDING_NEW`, …) |
| PendingAction | string | `NONE` / `NEW` / `CANCEL` / `REPLACE` (in-flight request) |
| PendingClOrdID | string | ClOrdID of the in-flight F/G ("" if none) |
| LastExecType | string | enum name of last 150 |
| CumQty, LeavesQty, AvgPx | double | venue absolute snapshots |
| LastShares, LastPx | double | most recent fill |
| LastMkt | string | 30 |
| OrdRejReason, CxlRejReason, DKReason | string | 103 / 102 / 127 (last seen) |
| Text | string | last 58 seen |
| ExecCount | long | # distinct ExecIDs |
| MsgCount | long | # messages applied to this chain |
| FirstSeenTs, LastUpdateTs | Instant | ingest bookkeeping |
| LastMsgType | string | `D/G/F/8/9/Q` |
| Terminal | bool | OrdStatus ∈ {FILLED, CANCELED, REJECTED, EXPIRED, DONE_FOR_DAY} |

## 5. State machine — transitions

`OrdStatus` on 8s **always comes from tag 39** (never forced from ExecType — a
replace-confirm on a partially filled order stays `PARTIALLY_FILLED`, venue says so via 39).
Requests move the order into local pending states; 8/9 resolve them.

```
             D                8(150=0,39=0)
  [*] ──► PENDING_NEW ─────────► NEW ──────────────► PARTIALLY_FILLED ──► FILLED
              │ 8(150=8,39=8)     │  8(150=1,39=1)        │    ▲               (terminal)
              ▼                   │                       │    │ 8(150=1)
           REJECTED (terminal)    │ 8(150=2,39=2)         ▼    │
                                  └────────► FILLED   (fills also legal in PENDING_*)
  NEW / PARTIALLY_FILLED ── F ──► PENDING_CANCEL ── 8(150=4,39=4) ──► CANCELED (terminal)
                                       │ 9(434=1) revert → prior status
  NEW / PARTIALLY_FILLED ── G ──► PENDING_REPLACE ── 8(150=5,39=…) ──► (status from 39;
                                       │ 9(434=2) revert → prior status   new terms live)
```

Rules (each numbered rule must have a unit test):

1. **D** creates the chain: seeds terms, `OrdStatus=PENDING_NEW`, `PendingAction=NEW`,
   `CumQty=0, LeavesQty=OrderQty, AvgPx=0`.
2. **8** resolves the chain (§3; may create it — audit feeds can start mid-stream:
   populate terms best-effort from the 8). Then:
   - `OrdStatus` ← tag 39 (mapped); `LastExecType` ← tag 150.
   - `CumQty/LeavesQty/AvgPx` ← tags 14/151/6 verbatim (absolute snapshots).
   - Fill reports (`150∈{1,2}` and `20∈{0,∅}`): set `LastShares/LastPx/LastMkt`.
   - **ExecID dedupe:** if tag 17 already seen for this chain, still bind ids and count
     the message, but apply **no** economic/status changes (replay guard).
   - `150=0` (ack): clears `PendingAction=NEW`.
   - `150∈{4}` / 39=4: clears a pending CANCEL. `150=5`: clears pending REPLACE and
     applies the staged G terms (price/qty/TIF), rotates `ClOrdID` (tag 11 becomes
     current; old id appended to `ClOrdIDChain`).
   - `150=8` (reject): terminal; `OrdRejReason` ← 103.
   - **Bust** (`20=1`): adopt restated 14/151/6/39; mark referenced ExecID busted (drives
     the executions panel); `LastShares/LastPx` untouched.
   - **Correct** (`20=2`): adopt restated snapshots; referenced ExecID marked corrected;
     the correcting report's 32/31 become the exec's current values.
   - Unsolicited `Canceled/Replaced` (no pending request recorded) is legal — accept.
   - **Stale guard:** a non-bust/correct 8 whose `CumQty` is **lower** than current is
     stale — bind ids, count it, skip economic fields.
3. **G** (amend request): resolve via 41→§3; register new ClOrdID alias; snapshot
   `prior_status[ClOrdID_of_G] = current OrdStatus`; `OrdStatus=PENDING_REPLACE`,
   `PendingAction=REPLACE`, `PendingClOrdID=11`; **stage** new terms (38/44/59) — do
   **not** apply until the confirming 8 (`150=5`).
4. **F** (cancel request): same resolution; snapshot prior status under the F's ClOrdID;
   `OrdStatus=PENDING_CANCEL`, `PendingAction=CANCEL`, `PendingClOrdID=11`.
5. **9** (cancel reject): resolve via 11/41. Revert `OrdStatus` to
   `prior_status[tag 11]` (per-request snapshots so multiple in-flight F/G don't clobber
   each other); if the 9 carries tag 39, the venue's value **wins** over the snapshot.
   `434` says which pending flag to clear (1→CANCEL, 2→REPLACE); discard that request's
   staged terms; `CxlRejReason` ← 102, `Text` ← 58.
6. **Q** (DK): resolve via 37/17. **No economic change.** Record `DKReason` ← 127; mark
   the referenced ExecID DK'd (executions panel). If the ExecID is unknown, still attach
   to the order if 37 resolves.
7. Every message (all types): bind ids, `MsgCount+=1`, `LastMsgType`, `LastUpdateTs`,
   append to message audit. Terminal orders still accept busts/corrects/DKs and late 8s
   (venue restatement can even reopen: e.g. bust after FILLED → 39=1 → PARTIALLY_FILLED).

## 6. Published event rows (drive the history/executions panels)

The state machine emits, per input message, besides the OrderState snapshot:

**`executions`** — one row per `35=8` (all of them, incl. acks — filterable) and per `Q`:

| Column | Notes |
|---|---|
| OrderKey, OrderID, ClOrdID, ExecID, ExecRefID | ids ("" when absent) |
| ExecTransType, ExecType, OrdStatus | enum names |
| LastShares, LastPx, CumQty, LeavesQty, AvgPx | doubles |
| LastMkt, Text | strings |
| IsFill | bool: `ExecType∈{PARTIAL_FILL,FILL}` and `20∈{0,∅}` |
| FillStatus | `NORMAL` / `BUSTED` / `CORRECTED` / `DK` — **latest** disposition of *this* ExecID; bust/correct/DK rows also re-emit the referenced exec's row with its new status so `last_by(ExecID)` shows current truth |
| TransactTime, IngestTs | Instants |

**`order_events`** — one row per lifecycle event (feeds the order-history panel):

| Column | Notes |
|---|---|
| OrderKey, ClOrdID, OrigClOrdID, OrderID | |
| EventType | `NEW_REQUEST`, `NEW_ACK`, `NEW_REJECT`, `AMEND_REQUEST`, `AMEND_ACK`, `AMEND_REJECT`, `CANCEL_REQUEST`, `CANCEL_ACK`, `CANCEL_REJECT`, `PENDING_NEW`, `PENDING_AMEND`, `PENDING_CANCEL`, `PARTIAL_FILL`, `FULL_FILL`, `FILL_BUST`, `FILL_CORRECT`, `DK_TRADE`, `RESTATED`, `STATUS`, `EXPIRED`, `DONE_FOR_DAY` |
| MsgType | 35 value |
| OrdStatus | resulting status (enum name) |
| OrderQty, Price | request terms where relevant (G shows proposed terms) |
| Detail | human-readable summary, e.g. `reject: too late to cancel (102=0)` |
| TransactTime, IngestTs | |

Event derivation from an 8: `150=0→NEW_ACK`, `A→PENDING_NEW`, `1→PARTIAL_FILL`,
`2→FULL_FILL`, `4→CANCEL_ACK`, `5→AMEND_ACK`, `6→PENDING_CANCEL`, `E→PENDING_AMEND`,
`8→NEW_REJECT`, `D→RESTATED`, `C→EXPIRED`, `3→DONE_FOR_DAY`; `20=1→FILL_BUST`,
`20=2→FILL_CORRECT` (ExecTransType wins over 150 for event naming). From a 9:
`434=1→CANCEL_REJECT`, `434=2→AMEND_REJECT`.

**`fix_messages`** — one row per raw message: OrderKey, MsgType, all §2 tags as typed
columns, `RawFix` (delimiters rendered as `|` for display), `ChecksumOk`, `SeqNum`,
`SendingTime`, `IngestTs`.

## 7. Edge cases the tests must cover

1. OrderID absent until first 8; searches by ClOrdID work in that window.
2. Amend chain `C1→C2→C3`: all ClOrdIDs resolve to the same OrderKey forever.
3. Duplicate ExecID replay → no double-count (CumQty unchanged).
4. Two in-flight requests (G then F before responses): a 9 for the F reverts only the
   cancel; the pending replace stays pending.
5. Reject-before-ack (first 8 is 150=8): terminal REJECTED, no NEW ever.
6. Bust after full fill: FILLED → PARTIALLY_FILLED per restated 39/14/151.
7. Correct changes price only: CumQty unchanged, AvgPx adopts restated tag 6.
8. DK on unknown ExecID but known OrderID: attaches, no economic change.
9. 8 before D (mid-stream start): chain created from the 8; late D merges, does not
   clobber venue state (terms fill only if empty; status untouched).
10. Fill arriving while PENDING_CANCEL/PENDING_REPLACE: quantities apply, status follows
    tag 39 of that fill (venue truth), pending flags remain until 8(4/5) or 9.
11. Unsolicited cancel (150=4 with no F): accepted.
12. Stale lower-CumQty 8 ignored economically.
