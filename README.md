# Sentinel BEAM Probe SDK (Elixir)

A Mix project implementing the BEAM Probe SDK for [Sentinel](https://github.com/emet-labs/sentinel),
mirroring the Go SDK (`sdk/go/`) module-for-module. This is issue #32.

## What it does

The SDK gives a BEAM Probe four things:

1. **Filter projection** (`Sentinel.Probe.SDK.Filter`) — relevance projection per ADR-0006.
2. **Enforcement gate** (`Sentinel.Probe.SDK.Enforcement.Gate`) — the one runtime gate
   (ADR-0023 gate 2): per-spec fail mode, fail-closed-wins, monotonic budget.
3. **OTLP log receiver** (`Sentinel.Probe.SDK.Otlp.Receiver`) — `POST /v1/logs` on
   `127.0.0.1` decodes Verdict, Event Filter, and DecideResponse. The enforcement
   gate reads DecideResponse from that log and does not call Connect.
4. **Emission** (`Sentinel.Probe.SDK.Emission`) — ended spans export as OTLP/HTTP
   protobuf to `/v1/traces` with `x-sentinel-source-handle`. Completeness is an
   OTLP log typed `sentinel.model.v1.ProducerCompletenessSignal`. A missing
   predecessor id fails the export; a span id is not stored as an Event id.

## Module map (Go → Elixir)

| Go                              | Elixir                                        |
| ------------------------------- | --------------------------------------------- |
| `filter/apply.go`               | `Sentinel.Probe.SDK.Filter`                   |
| `enforcement/gate.go`           | `Sentinel.Probe.SDK.Enforcement.Gate`         |
| `enforcement/budget.go`         | `Sentinel.Probe.SDK.Enforcement.Budget`       |
| `client/client.go`               | `Sentinel.Probe.SDK.Client.Client`            |
| `client/filterstore.go`          | `Sentinel.Probe.SDK.Client.FilterStore`        |
| `client/transport.go`            | `Sentinel.Probe.SDK.Client.Transport`         |
| `emission/spantoevent.go`        | `Sentinel.Probe.SDK.Emission.SpanToEvent`      |
| —                                | `Sentinel.Probe.SDK.Otlp.Exporter`             |
| —                                | `Sentinel.Probe.SDK.Otlp.Receiver`             |
| `emission/tracer.go`             | `Sentinel.Probe.SDK.Emission.Tracer`          |
| `config/sourcetier.go`           | `Sentinel.Probe.SDK.Config.SourceTier`        |
| `int128/int128.go`               | `Sentinel.Probe.SDK.Int128`                   |
| `ids/ids.go`                     | `Sentinel.Probe.SDK.IDs`                      |
| `internal/specmatch/specmatch.go`| `Sentinel.Probe.SDK.Internal.SpecMatch`       |

## Sequence numbers and completeness

`Sentinel.Probe.SDK.Emission.Sequence` is a GenServer, one per source. Start it with
`Sequence.start_link/1` and pass its pid as `:allocator` to the emission processor.
The processor allocates at span start and stamps `sentinel.sequence.epoch` and
`sentinel.sequence.number`. A host-stamped span keeps its slot. After the export,
only HTTP 2xx with zero `partial_success.rejected_spans` marks the slot exported;
a refused, failed or undecodable response leaves a hole. Hosts that export manually
use `allocate/1`, `mark_exported/2`, `mark_failed/2` and `mark_filtered/3`.

`Sentinel.Probe.SDK.Otlp.Heartbeat` reports the allocator on `/v1/logs`:

```elixir
{:ok, heartbeat} = Heartbeat.start_link(
  sequence: sequence,
  otlp: %{endpoint: sentinel_url, source_handle: source_handle},
  filter_epoch: fn -> Client.acknowledged_epoch(probe) end
)
```

The host owns supervision and registration of the processor. Heartbeat reports every
15 seconds by default; `:interval_ms` accepts `1000..59999`, or `0` for manual
`report/1` only. `:now`, `:schedule` and `:send` inject clock, scheduler and transport.
An absent Filter is acknowledged as epoch 0. `declare_idle/1` declares intentional
idle; the next allocation or `declare_active/1` makes the source active again.

`last_emitted` is the highest allocated number; `complete_through` advances only over
the contiguous successfully exported prefix. An in-flight, failed or Filter-dropped
slot holds the boundary back, so missing evidence remains Inconclusive. Filtered
ranges retain the Filter epoch for #542, but are not yet declared on the wire and do
not advance completeness. The helper does not retry exports.

A report with equal counters declares the epoch closed. The allocator seals it, and
the next allocation opens `max(previous epoch + 1, current Unix milliseconds)`.
It never adds Events to a declared close. The heartbeat restates the previous epoch's
final declaration before the new one until accepted under the current Filter epoch.
Only the epoch current at Filter publication can settle Authoritative; Events that
straddle a source's epoch roll may leave their Binding Inconclusive.

On restart, use a strictly greater epoch. The default is Unix milliseconds; if the
clock can step backwards, persist the last epoch and pass `:previous_epoch` to
`Sequence.start_link/1`. A clock-derived epoch then advances past it; an explicit
`:epoch` that is not greater is refused. Without persistence a regression is refused
by Sentinel with HTTP 409.

`Heartbeat.stop/1` cancels scheduling and sends nothing further, including a queued
second signal. `shutdown/1` waits for a running report and sends a final declaration,
which closes only if every allocated slot was exported. Let exports settle first on
a clean exit. Both leave the GenServer inert for the supervisor to stop.

## Hardened span export

`Processor` is safe inside a host: `on_start/3` and `on_end/2` catch every error and exit, mark the slot
failed, report to `:on_export_error` and a rate-limited log, and never raise. With `:otlp` configured the OTel
provider starts a bounded `Otlp.AsyncExporter` (queue, worker, retry with backoff on transport errors, 5xx and
429; failed slots are retried on a timer and at flush; an overflow drops the new Event and fails its slot).
`AsyncExporter.shutdown(exporter, heartbeat: hb)` flushes before the final heartbeat.

- `allow_unattributed_remote_parent: true` exports a span with a remote, non-Probe parent as a causal root (no
  parent span id on the wire). A local parent without `sentinel.parent.event.id` is still refused.
- `Propagation.inject_parent_event_id/2` / `extract_parent_event_id/1`: header `x-sentinel-parent-event-id`,
  `tracestate` `sentinel=<eventId>` as the extract fallback.
- `filter_store:` applies the held Event Filter on the span path (unselected Events mark their slot filtered).
- `resource_attributes:` sets OTLP Resource attributes; `Receiver.start_link(ip: {0,0,0,0}, port: 4319)` binds
  for a containerised Probe (default stays loopback, ephemeral port).

## Generated proto modules

`gen/` is gitignored and produced by `tools/generate-elixir-sdk.sh`, mirroring
`sdk/go/gen/`. Nothing under `sdk/elixir` compiles without it.

## Development

```sh
tools/generate-elixir-sdk.sh   # populate gen/
cd sdk/elixir
mix deps.get
mix compile
mix test
mix format --check-formatted
```

## Non-goals

No live SagaShop integration (#22/#23), no committed generated code, no runtime
safety/anchor gate re-checks (ADR-0023 gates 1,3,4,5 are promotion-time only).
