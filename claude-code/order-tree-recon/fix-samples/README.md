# FIX order-state samples

The mock book (`OTR_SEED=42`, `OTR_MOCK_FAMILIES=24`) written as the FIX order-state messages
each system would publish to its AMPS SOW topics during the day, one file per topic. They
are what `OTR_SOURCE=fix-files` reads by default (compose mounts this folder at `/otr-fix`),
what `run_demo.sh publish` loads into a local AMPS, and the mapping `OTR_SOURCE=amps` uses
when you don't supply your own. The FIX layer is described in
[docs/14 section 12](../../docs/14-order-tree-reconciliation.md#12-fix-order-states-from-amps-sow-topics).

| File (= SOW topic) | System | Orders |
|---|---|---|
| `raptor.orders.fix` | `RAPTOR` | client orders: `37` Raptor OrderID, `11` the client's ClOrdID, `1` moniker (Account) |
| `algo.parent.orders.fix` | `ALGO` | algo parents: `11` = the Raptor OrderID they were sent with, `847` strategy |
| `algo.child.orders.fix` | `ALGO` | algo slices: `20001` = the parent's OrderID |
| `router.parent.orders.fix` | `OR` | router parents: `11` = the algo child id, or the client ClOrdID for DMA |
| `router.child.orders.fix` | `OR` | venue orders: `20001` = the router parent, `100` venue |
| `fix_config.json` | | the mapping these follow (= `fixstate.DEFAULT_FIX_CONFIG`) |

One message per line, each the order's **complete** state, as AMPS delivers a `fix` topic:
a body without the `8=` / `9=` / `10=` framing. They use `|` for readability; SOH, `|` and `^A`
are all accepted, and so is a log prefix. Each order sends a `NEW` state, a partial fill, its
final fill and, if the scenario killed it, a `CANCELED` state, so the files read like a
recorded `sow_and_subscribe`, and the latest message per order wins.

The `dma` client order `RPT-00003` is cancel/replaced mid-day (`11=BLUEFIN-00003-R1`,
`41=BLUEFIN-00003`). Its router parent `OR-00007` keeps the original ClOrdID, so that link
resolves only through Raptor's OrigClOrdID / id history.

Do not edit by hand. Regenerate from `order-tree-recon/`:

```bash
PYTHONPATH=src python -m order_tree_recon.fixstate_mock --out fix-samples
```

`tests/test_fixstate.py` fails if these files drift from the generator.
