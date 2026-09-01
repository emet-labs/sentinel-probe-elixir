# Elixir Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-elixir/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-elixir/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The Elixir Probe SDK for [Sentinel](https://github.com/emet-labs) — instrument a BEAM
application as a Sentinel **Probe**: the in-process component that reports what your
application really did, and asks Sentinel whether to proceed before an action becomes
irreversible.

Sentinel verifies cross-system action sequences against rules you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract.

## Requirements

- Elixir 1.16+ on Erlang/OTP 28.
- A running [Sentinel](https://github.com/emet-labs) deployment to connect to.
- The SDK is library code: it starts no processes of its own. Your application owns the
  Finch pool and the OpenTelemetry TracerProvider and exporter.

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

```elixir
alias Sentinel.Probe.SDK.Client
alias Sentinel.Probe.SDK.Enforcement.Gate

# Connect to Sentinel's decision endpoint and identify this Probe. The decider
# carries requests over Connect application/proto (Finch) — see Client.Transport.
decider = Client.transport_decider("http://sentinel.local:7070", MyApp.Finch)
probe = Client.new(%Client.Config{source_handle: "checkout-service"}, decider)

# The tracer your application emits evidence with. The host owns the OTel
# provider and must register the SDK's span processor with it.
tracer =
  Sentinel.Probe.SDK.Emission.Tracer.new(%Sentinel.Probe.SDK.Emission.Tracer.Options{
    tracer_name: "checkout-service",
    provider: host_provider
  })

# ... start a span under the tracer, do the work the Specification watches,
# and let the processor capture the ended span ...

# Convert the ended span into the event Sentinel reasons about.
event = Sentinel.Probe.SDK.Emission.SpanToEvent.span_to_event(ended_span)

# Ask before the action becomes irreversible.
outcome =
  Sentinel.Probe.SDK.gate(
    event,
    Client.current_filter(probe),
    nil,  # deadline_ns: a monotonic deadline, if you set one
    %Gate.Deps{
      decide: Client.decide_func(probe),
      now_monotonic_ns: &:erlang.monotonic_time(:nanosecond)/0,
      accepted_fail_mode_for: &accepted_fail_mode/1  # what you contracted to
    },
    %Gate.Options{
      source_handle: "checkout-service",
      request_id: Sentinel.Probe.SDK.generate_request_id(),
      idempotency_key: Sentinel.Probe.SDK.generate_idempotency_key()
    }
  )

case outcome.kind do
  kind when kind in [:permit, :fail_open_permit, :no_filter] ->
    # No filter held means the conservative default: proceed, fail-open.
    commit_the_action()

  _other ->
    # deny | defer | fail_closed: block, or retry within the budget.
    rollback()
end
```

When Sentinel publishes a new Event Filter epoch for this source, swap it in:

```elixir
Client.set_filter(probe, new_filter)  # a no-op when the epoch is unchanged
```

## What the Probe does

| Module | Duty |
|---|---|
| `Sentinel.Probe.SDK.Client` | Holds the versioned Event Filter for your source; the Connect-over-Finch decision transport |
| `Sentinel.Probe.SDK.Filter` | Relevance projection before shipping — drops attributes no Specification needs. Never samples (`apply_filter/2`) |
| `Sentinel.Probe.SDK.Emission` | OTel spans become Sentinel events (`Tracer`, `span_to_event/1`) |
| `Sentinel.Probe.SDK.Enforcement.Gate` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`gate/5`) |
| `Sentinel.Probe.SDK.Config.SourceTier` | Source-tier resolution from deployment config, never hard-coded |

Everything is also reachable through the `Sentinel.Probe.SDK` module.

## Status

Early, pre-1.0. The wire protocol is versioned per release, but the API surface may
still change.

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
