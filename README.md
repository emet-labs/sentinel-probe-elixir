# Sentinel BEAM Probe SDK (Elixir)

A Mix project implementing the BEAM Probe SDK for [Sentinel](https://github.com/emet-labs/sentinel),
mirroring the Go SDK (`sdk/go/`) module-for-module. This is issue #32.

## What it does

The SDK gives a BEAM Probe four things:

1. **Filter projection** (`Sentinel.Probe.SDK.Filter`) — relevance projection per ADR-0006.
2. **Enforcement gate** (`Sentinel.Probe.SDK.Enforcement.Gate`) — the one runtime gate
   (ADR-0023 gate 2): per-spec fail mode, fail-closed-wins, monotonic budget.
3. **Decision transport** (`Sentinel.Probe.SDK.Client.Transport`) — Connect
   `application/proto` (binary) over Finch, not Connect-JSON.
4. **Emission** (`Sentinel.Probe.SDK.Emission`) — wraps the OTel Erlang SDK; a custom
   span processor captures ended spans and `span_to_event/1` converts them to
   `ProducerEvent`s. The host owns export (ADR-0002).

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
| `emission/tracer.go`             | `Sentinel.Probe.SDK.Emission.Tracer`          |
| `config/sourcetier.go`           | `Sentinel.Probe.SDK.Config.SourceTier`        |
| `int128/int128.go`               | `Sentinel.Probe.SDK.Int128`                   |
| `ids/ids.go`                     | `Sentinel.Probe.SDK.IDs`                      |
| `internal/specmatch/specmatch.go`| `Sentinel.Probe.SDK.Internal.SpecMatch`       |

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
