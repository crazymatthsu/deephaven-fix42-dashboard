# FIX drop-copy samples

What `OTR_SOURCE=fix-files` reads by default (compose mounts this folder at `/otr-fix`).
They are the mock book (`OTR_SEED=42`, `OTR_MOCK_FAMILIES=24`) written as FIX 4.2
ExecutionReports, so the dashboard looks the same in both modes. The FIX layer and the
config format are described in
[docs/14 section 12](../../docs/14-order-tree-reconciliation.md#12-real-fix-messages).

| File | System | What it shows |
|---|---|---|
| `raptor.log` | `RAPTOR` | client orders: `37` = Raptor OrderID, `11` = the client's ClOrdID, `1` = moniker (first report only), inbound `35=D`s (skipped) |
| `algo.log` | `ALGO` | algo parents (`11` = the Raptor OrderID they were sent with), algo children (`20001` = parent OrderID), `847` = strategy |
| `router.log` | `OR` | router parents (`11` = the algo child id, or the client ClOrdID for DMA), venue orders (`20001` = router parent), `100` = venue |
| `fix_config.json` | | the tag mapping these files follow (= `fix.DEFAULT_FIX_CONFIG`) |

Line format: QuickFIX log style, `|` instead of SOH for readability (SOH, `|` and `^A`
are all accepted, and so is any prefix before `8=FIX`):

```
20261009-13:30:00.035 : 8=FIX.4.2|9=164|35=8|49=RAPTOR|56=DROPCOPY|34=3|...|37=RPT-00001|11=ACME-00001|...|10=051|
```

The `dma` client order `RPT-00003` is cancel/replaced (`11=BLUEFIN-00003-R1`,
`41=BLUEFIN-00003`) and so is its router parent `OR-00007`. The router parent still links
through its original ClOrdID, which resolves only via Raptor's id history.

Do not edit by hand. Regenerate with (from `order-tree-recon/`):

```bash
PYTHONPATH=src python -m order_tree_recon.fix_mock --out fix-samples
```

`tests/test_fix.py` fails if these files drift from the generator.
