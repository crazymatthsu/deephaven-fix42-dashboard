# Order-state samples

What `OTR_SOURCE=state-files` reads by default (compose mounts this folder at `/otr-states`).
They are the mock book (`OTR_SEED=42`, `OTR_MOCK_FAMILIES=24`) written as the order-state
messages each system would have sent during the day, so the dashboard looks the same in
both modes. The mapping and the formats are described in
[docs/14 section 12](../../docs/14-order-tree-reconciliation.md#12-order-state-messages).

| File | System | Format | What it shows |
|---|---|---|---|
| `raptor.csv` | `RAPTOR` | CSV | client orders; `Side` in FIX codes (`side_codes: "fix"`); latest = last row (no sequence) |
| `algo.jsonl` | `ALGO` | JSON Lines | parents (`RaptorOrderId`) and children (`ParentAlgoOrderId`) in one feed, fields left out when not applicable; `sequence: "Version"` (numeric) |
| `router.csv` | `OR` | CSV | parents (`UpstreamOrderId` = algo child id, or the client ClOrdID for DMA) and venue orders (`ParentRouterOrderId`); `sequence: "UpdateTime"` (ISO-8601 text) |
| `state_config.json` | | | the field mapping these files follow (= `states.DEFAULT_STATE_CONFIG`) |

Each order sends a `NEW` state, a partial fill, its final fill and, if the scenario killed
it, a `CANCELED` state. Every message is the order's complete state. The `dma` client
order `RPT-00003` changes ClOrdID mid-day (`BLUEFIN-00003` → `BLUEFIN-00003-R1`), while
its router parent `OR-00007` keeps the id it was sent with. That link resolves only
through Raptor's id history.

Do not edit by hand. Regenerate from `order-tree-recon/`:

```bash
PYTHONPATH=src python -m order_tree_recon.states_mock --out state-samples
```

`tests/test_states.py` fails if these files drift from the generator.
