# Elixir Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-elixir/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-elixir/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The Elixir Probe SDK for [Sentinel](https://github.com/emet-labs) — instrument a BEAM
application as a Sentinel **Probe**: the in-process component that reports what your
application really did, as Events, and receives Sentinel's Verdicts and Event Filters in
return.

Sentinel verifies cross-system action sequences against Specifications you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract. Missing evidence is never a pass: until Sentinel can show it has seen everything
a source emitted, a Verdict that depends on that source stays Inconclusive.

## Requirements

- Elixir 1.16+ on Erlang/OTP 28.
- A [Sentinel](https://github.com/emet-labs) deployment on the same host as the Probe.
  The receiver uses the IPv4 loopback address `127.0.0.1`. Start it before Sentinel,
  following the order below.
- The SDK is library code: it starts no processes of its own. Your application owns the
  OpenTelemetry TracerProvider, and starts the SDK's servers (the log receiver, the
  sequence allocator and the completeness heartbeat) where it supervises them.

## Installation

The package is not yet on Hex. Until it is, use it as a Git dependency:

```elixir
def deps do
  [
    {:sentinel_probe_sdk,
     git: "https://github.com/emet-labs/sentinel-probe-elixir.git"}
  ]
end
```

The generated protobuf modules are committed, so a bare clone compiles with no code
generator.

## Quickstart

The Probe wire is OTLP/HTTP protobuf, in both directions:

- each ended OTel span is exported as one Event to Sentinel's `POST /v1/traces`, with the
  `x-sentinel-source-handle` header naming your source;
- completeness signals go to `POST /v1/logs` on the same base URL, with the same
  `x-sentinel-source-handle` header;
- Sentinel posts Verdicts and published Event Filters as OTLP logs to the Probe's loopback
  log receiver.

Start the Probe and its log receiver first. Then start Sentinel with
`SENTINEL_PROBE_LOG_URL` set to the receiver's full `http://127.0.0.1:<port>/v1/logs`
URL, and publish the Specification only after the receiver is listening. Sentinel reads
this URL once at startup and does not retry a failed Filter push. If the receiver's port
changes when the Probe restarts, restart Sentinel with the new URL and publish again.
One Sentinel process has one Probe log URL, so only one Probe process receives these
Verdicts and Event Filters today.

Both ingest routes require a source handle admitted by your Sentinel deployment; replace
`checkout-service` below with that handle. A missing header returns HTTP 401; an unknown
handle returns HTTP 403, and neither stores evidence. A completeness record's
`sentinel.source_handle` must match the header; the SDK exporter sets both.

Sentinel accepts `Content-Type: application/x-protobuf`, with identity or gzip encoding.
OTLP/JSON, OTLP/gRPC and other encodings are unsupported (HTTP 415 on the HTTP route).
An invalid span can refuse the whole request during decoding, including an unknown
`sentinel.*` attribute. Admission conflicts may instead return HTTP 200 with nonzero
`partial_success.rejected_spans`; this is still a failed export for completeness.
The OTel span name becomes the Event kind.

```elixir
alias Sentinel.Probe.SDK.Client.Client
alias Sentinel.Probe.SDK.Emission.Sequence
alias Sentinel.Probe.SDK.Otlp.{Heartbeat, Receiver}
require OpenTelemetry.Tracer, as: Tracer

sentinel_url = "http://127.0.0.1:8080"
source_handle = "checkout-service"

# The loopback log receiver: point Sentinel's SENTINEL_PROBE_LOG_URL at
# Receiver.origin(receiver) <> "/v1/logs".
{:ok, receiver} = Sentinel.Probe.SDK.start_log_receiver()

# Passing the receiver pid wires both directions: each Event Filter it accepts replaces
# the client's Filter, and the gate waits on it for decisions.
probe = Client.new(%Client.Config{source_handle: source_handle}, receiver)

# One sequence allocator per source (see "Sequence numbers and completeness").
{:ok, sequence} = Sequence.start_link([])

# Register {Sentinel.Probe.SDK.Emission.Processor, processor_config} as a span processor
# with your TracerProvider. With :otlp it exports each ended OTel span as one Event to
# POST /v1/traces; with :allocator it stamps sentinel.sequence.* when each span starts.
# Install this tuple in your OpenTelemetry TracerProvider's processor list at startup,
# after sequence and probe exist. This example constructs it; your provider must register it.
processor_config = %{
  otlp: %{endpoint: sentinel_url, source_handle: source_handle},
  allocator: sequence,
  schema_version: "1.0.0",
  claimed_sensitivity: :SENSITIVITY_INTERNAL,
  acknowledged_epoch: fn -> Client.acknowledged_epoch(probe) || 0 end,
  on_export_error: &log_export_error/1 # your handler for refused and failed exports
}

# Declares this source's completeness on POST /v1/logs.
{:ok, heartbeat} = Heartbeat.start_link(
  sequence: sequence,
  otlp: %{endpoint: sentinel_url, source_handle: source_handle},
  filter_epoch: fn -> Client.acknowledged_epoch(probe) end,
  on_error: fn error -> IO.inspect(error, label: "completeness failed") end
)

Tracer.with_span "checkout.charge.completed", %{
  attributes: %{"sentinel.event.id" => UUID.uuid4(), "order.id" => "order-1001"}
} do
  # ... the work the Specification watches ...
end

# On a clean exit, once the exports have settled: send the final completeness report.
Heartbeat.shutdown(heartbeat)
```

The processor builds each Event from the ended OTel span and its config: the span supplies
`sentinel.event.id`, and optionally `sentinel.parent.event.id` and links that carry their
own `sentinel.event.id`; the config supplies the schema version, the acknowledged Filter
epoch (an integer, 0 until the first Event Filter arrives), the claimed sensitivity (a
`Sensitivity` atom other than `:SENSITIVITY_UNSPECIFIED`) and the sequence slot. A config
value may be a zero-arity function, resolved when the span ends. An OTel span that breaks
the Event contract is not sent; that refusal, and an export Sentinel refuses, both reach
`:on_export_error`. A span id never stands in for a missing Event id.

`Client.new/2` with the receiver pid attaches the client's filter store, so each Event
Filter the receiver accepts is swapped in (a no-op when the epoch is unchanged or older). The
processor does not apply the Event Filter: it exports every ended OTel span in full.
`Sentinel.Probe.SDK.apply_filter/2` projects a `ProducerEvent` against the held Filter.

Start Probe spans as roots, or stamp the parent's Event id in
`sentinel.parent.event.id`: an ambient OTel parent without that Event id is refused.

### Enforcement gate

The gate is in the SDK, but Sentinel does not answer decision requests yet (see Status).

```elixir
alias Sentinel.Probe.SDK.Enforcement.Gate

# `event` is the ProducerEvent for the action, for example the one the processor hands
# to an :on_event callback. Ask before the action becomes irreversible.
outcome =
  Sentinel.Probe.SDK.gate(
    event,
    Client.current_filter(probe),
    nil, # deadline_ns: an absolute monotonic deadline, if you set one
    %Gate.Deps{
      decide: Client.decide_func(probe),
      now_monotonic_ns: fn -> :erlang.monotonic_time(:nanosecond) end,
      accepted_fail_mode_for: &accepted_fail_mode/1 # the fail mode your deployment contracted
    },
    %Gate.Options{
      source_handle: source_handle,
      request_id: Sentinel.Probe.SDK.generate_request_id(),
      idempotency_key: Sentinel.Probe.SDK.generate_idempotency_key()
    }
  )

# :permit, :fail_open_permit and :no_filter proceed; :deny, :defer and :fail_closed_deny do not.
if Gate.permitted?(outcome.kind), do: commit_the_action(), else: rollback()
```

## What the Probe does

| Module | Duty |
|---|---|
| `Sentinel.Probe.SDK.Otlp` | The Probe wire. `Exporter` posts Events to `POST /v1/traces` and completeness signals to `POST /v1/logs`; `Receiver` is the loopback log receiver for Verdicts, Event Filters and DecideResponses; `Heartbeat` reports completeness |
| `Sentinel.Probe.SDK.Emission` | OTel spans become Events (`Processor`, `Tracer`, `SpanToEvent`). `Sequence` numbers the Events |
| `Sentinel.Probe.SDK.Client.Client` | Holds the versioned Event Filter for your source and waits on the log receiver for decisions |
| `Sentinel.Probe.SDK.Filter` | Relevance projection — drops attributes no Specification needs. Never samples (`apply_filter/2`) |
| `Sentinel.Probe.SDK.Enforcement.Gate` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`gate/5`). Not answered yet: see Status |
| `Sentinel.Probe.SDK.Config.SourceTier` | Source Tier resolution from deployment config, never hard-coded |

The `Sentinel.Probe.SDK` module also delegates the filter, the gate, source-tier config,
the identifier generators, trace and completeness export, and `start_log_receiver/1`.

## Sequence numbers and completeness

To show it has seen everything a source emitted, Sentinel needs two things from the
source:

- **Sequence numbers.** Every exported Event carries `sentinel.sequence.epoch`, its
  Sequence Epoch, and `sentinel.sequence.number`: 1, 2, 3, … within that epoch.
- **Completeness signals.** A `ProducerCompletenessSignal` on `POST /v1/logs` declares, for
  the current epoch, `last_emitted` (the highest number allocated) and `complete_through`
  (the highest number up to which every Event was exported successfully), with the Filter
  epoch the Probe acknowledges. The source's Watermark advances from these signals.

The SDK provides both:

- `Sentinel.Probe.SDK.Emission.Sequence`, a GenServer: start one per source with
  `start_link/1` and give it to the processor as `:allocator`. The processor then
  allocates a slot when each OTel span starts and uses it for that span's Event.
- After each export the processor marks the slot. An export succeeded only when Sentinel
  answered HTTP 2xx with no rejected spans (`partial_success.rejected_spans` absent or 0;
  `Sentinel.Probe.SDK.Otlp.PartialSuccess.rejected_spans/1` decodes that count); anything
  else marks the slot failed. If you allocate a slot and then drop its Event for the Event
  Filter, mark it with `mark_filtered/3`.
- `Sentinel.Probe.SDK.Otlp.Heartbeat`, a GenServer started with `start_link/1`, reports
  the allocator's declaration every 15 seconds by default; `report/1` sends one on demand.
  It acknowledges the held Filter epoch (0 before the first Event Filter), sends one signal
  at a time, and restates the previous signal when nothing has changed. `:now`,
  `:schedule` and `:send` replace its clock, its timer and its transport.

**`complete_through` never runs ahead of the evidence.** It advances only across a
contiguous run of successful exports. A failed export, an OTel span that never ends, and
today an Event dropped for the Event Filter each hold it below that slot: the epoch stays
open and the Verdicts that depend on this source stay Inconclusive instead of passing.
The allocator does not retry. Re-export the identical Event and call `mark_exported/2`
for its slot.

**The epoch rolls after a declared close.** A signal whose `complete_through` equals
`last_emitted` is a declared close, and Sentinel accepts nothing more in that epoch. The
allocator seals it, and the next allocation opens a new epoch: the greater of the previous
epoch plus one and the clock's Unix milliseconds. Expect a new Sequence Epoch after every
quiet period. If Sentinel has not yet accepted the previous epoch's final declaration
under the Filter epoch the Probe now acknowledges, the heartbeat restates it before the
first signal of the new epoch.

**A restarted process needs a strictly greater epoch.** By default the first epoch is the
clock's Unix milliseconds. If your clock can step backwards across a restart, persist the
last epoch and pass it to the next run's `Sequence.start_link/1` as the previous epoch:
the allocator then starts at the greater of the clock and that epoch plus one, and
refuses an explicit epoch that is not greater. Without it, a clock that stepped backwards
yields an epoch that Sentinel refuses with HTTP 409.

**Idle sources.** `declare_idle/1` reports the source as intentionally idle. The next
allocation, or `declare_active/1`, makes it active again.

**`stop/1` and `shutdown/1`.** `Heartbeat.stop/1` cancels the schedule and sends nothing
more: use it when the process must go silent. `Heartbeat.shutdown/1` waits for a running
report and sends one final report, which is a declared close only if every slot was
exported. On a clean exit, let the exports settle first, then call `shutdown/1`.

## Status

- **Detect-only.** Sentinel ingests Events, evaluates Specifications and reports Verdicts.
  It blocks nothing.
- **Decide is not served.** Sentinel answers no decision requests today. When an enforcing
  Specification selects an Event, the gate waits on the log receiver for a DecideResponse
  for the effective blocking budget in whole milliseconds (no wait under 1 ms): the
  smallest Enforcement Latency Budget among the selecting Specifications, capped by
  your deadline, sent as the request's Transport Budget. No answer arrives, so the gate then resolves by its fail mode:
  `:fail_closed_deny` when one of those Specifications is fail-closed and your deployment
  accepted that, `:fail_open_permit` otherwise.
- **One wire.** OTLP/HTTP protobuf, as described above, is the only Probe wire.
- **Unversioned.** There is no release yet, and no compatibility guarantee before one: the
  API and the wire may still change. A `0.1.0` package manifest is not a release.

A Specification with no blocking budget or a zero budget resolves by fail mode at once.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This repository is a published mirror; pull
requests are reviewed and re-landed upstream.

## License

[MPL-2.0](LICENSE)

## Development environment (this mirror): Devbox + just

This repository pins its own toolchain — `devbox.json` + `devbox.lock` — and every task
runs inside it, the same convention as the Sentinel source repository:

    devbox install        # once; resolves devbox.lock
    devbox shell          # then `just --list` for the recipes

`build`, `test`, `lint` and `fmt-check` reuse the canonical gate names of the source
repository, scoped to this one language.

The toolchain is tested with Devbox 0.19.0. Devbox release installs are a launcher that
runs `DEVBOX_USE_VERSION` when it is set, so `export DEVBOX_USE_VERSION=0.19.0` keeps you
on the tested version.
