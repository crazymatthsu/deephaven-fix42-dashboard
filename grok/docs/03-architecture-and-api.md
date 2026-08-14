# Architecture and public API

## Gradle layout

```
settings.gradle.kts          root name: deephaven-fix42-dashboard
build.gradle.kts             Java 21, JUnit 5, shared config
gradle/libs.versions.toml

fix-codec/                   no third-party deps
oms-engine/                  depends on :fix-codec
fix-demo-producer/           depends on :fix-codec + kafka-clients
dh-app/                      Python + Application Mode (no Java compile)
```

No Spring, no QuickFIX, no database. The artifact Deephaven loads is
the shadowed / assembled `oms-engine` JAR (transitively includes
`fix-codec`).

## Runtime flow

```
          raw FIX string (Kafka value)
                    │
                    ▼
               FixParser  ── dictionary ──► FixMessage
                    │
                    ▼
              OmsCache.ingest
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
   OrderLinker  StateUpdater  Indexes
         │          │            │
         └──────────┴────────────┘
                    │
                    ▼
              OrderState + ProcessResult
                    │
                    ▼
         DynamicTableWriter (Python)
                    │
                    ▼
         Deephaven DAG (doc 02)
```

### `fix-codec`

| Class | Responsibility |
|-------|----------------|
| `Tags` | Integer constants for every tag we name |
| `FixConstants` | SOH, `FIX.4.2`, MsgType chars |
| `FixField` / `FixMessage` | Ordered tag/value list + raw |
| `FixParser` | `|` or SOH split; optional 9/10 checks |
| `FixSerializer` | Rebuilds `9` and `10` |
| `FixParseException` | Malformed input |

Parser rules:

- Input may use `|` or SOH. Tests and Kafka demo use `|`. Output of
  `FixSerializer` always uses SOH.
- `8` first, `9` second, `35` third, `10` last when `strictHeader=true`.
- BodyLength and CheckSum validated when `validateChecksum=true`.
  Hand-written tests can disable both or go through `FixSerializer`.

Repeating groups are preserved as consecutive fields on `FixMessage`.
v1 does not project them into nested objects; the state machine only
reads scalar tags.

### `oms-engine`

| Class | Responsibility |
|-------|----------------|
| `OmsCache` | Public façade |
| `CacheConfig` | history, stale-ER policy, parent tags, validation |
| `InMemoryOmsCache` | Maps + lock + dispatch |
| `OrderLinker` | Identifier resolution (doc 01) |
| `OrderStateUpdater` | Field-level apply |
| `OrderState` | Latest-state POJO (not protobuf) |
| `ProcessResult` | After-apply snapshot for DH writers |

Thread safety: one `ReentrantReadWriteLock`. Ingest takes the write
lock; lookups take the read lock. Kafka listener is one writer.

`OrderState` is a mutable POJO with a `copy()` used when publishing.
Protobuf was the right store for the sibling library (binary snapshot).
Here the store is Deephaven columns; a POJO avoids a codegen plugin.

## Public API

```java
public interface OmsCache {
    ProcessResult ingest(String rawFix);

    Optional<OrderState> get(String orderKey);
    Optional<OrderState> getByClOrdId(String clOrdId);
    Optional<OrderState> getByOrderId(String orderId);
    Optional<OrderState> getByExecId(String execId);

    List<OrderState> findByAccount(String account);
    List<OrderState> findBySymbol(String symbol);

    List<OrderState> getChildren(String parentOrderId);
    Optional<OrderState> getParent(String childOrderKey);
    ChildRollup rollup(String parentOrderId);

    List<String> getHistory(String orderKey);
    Collection<OrderState> snapshot();
    int size();
}

public final class ProcessResult {
    String orderKey;
    String previousOrderKey;   // non-null on rekey
    OrderState state;          // after apply (null on tombstone-only)
    boolean created;
    boolean applied;           // false for stale ER / H-only
    boolean tombstone;         // old key after rekey
    String rawFix;
}
```

## Errors

- Unparseable FIX → `FixParseException`. `ingest` does not swallow it.
- Unknown `MsgType` → `UnsupportedMessageTypeException`.
- Missing both `ClOrdID` and `OrderID` on a state-changing message →
  `UnidentifiableOrderException`.
- `35=H` is accepted; `applied=false`.

The Deephaven listener catches these, writes a row to `fix_errors`
(append), and continues. One bad message must not kill the DAG.

## Configuration defaults

```
historyLimit            = 32
validateChecksum        = true
strictHeader            = true
applyStaleExecReports   = false
parentOrderIdTag        = 20001
parentClOrdIdTag        = 20002
```

## Kafka contract (demo + production)

| Item | Value |
|------|-------|
| Topic | `fix42.dropcopy` |
| Key | optional string (`ClOrdID`) |
| Value | raw FIX 4.2 (`|` or SOH) |
| Bootstrap (in compose) | `redpanda:9092` |
| Bootstrap (host producer) | `localhost:19092` |
| Offset (demo) | `ALL_PARTITIONS_SEEK_TO_BEGINNING` so restart rebuilds the book |

## Key decisions

1. **Java engine + Deephaven projection**, not a pure-table state machine.
2. **POJO `OrderState`**, not protobuf — DH columns are the interchange.
3. **Blink Kafka + append writers + `last_by`**, matching DH table-type
   guidance (blink for ingest, append for history, `last_by` for SOW).
4. **Tombstone on rekey** so `last_by("OrderKey")` does not leave a
   stale `C1` row beside `O9`.
5. **Redpanda** as the Kafka-compatible broker in the official
   Deephaven compose pattern; run via Podman Compose.
