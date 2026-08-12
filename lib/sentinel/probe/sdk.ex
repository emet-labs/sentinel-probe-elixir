defmodule Sentinel.Probe.SDK do
  @moduledoc """
  Public API entry point for the Sentinel BEAM Probe SDK.

  Delegates to the per-concern modules; a host wires them together. See the
  README for the Go → Elixir module map. The SDK is library code — it starts no
  processes of its own (the host owns the Finch pool and the OTel
  TracerProvider/exporter, per ADR-0002).
  """

  # Filter projection (ADR-0006).
  defdelegate apply_filter(event, filter), to: Sentinel.Probe.SDK.Filter

  # Enforcement gate (ADR-0023 gate 2).
  defdelegate gate(event, filter, deadline_ns, deps, options),
    to: Sentinel.Probe.SDK.Enforcement.Gate

  # Source-tier config (ADR-0022).
  defdelegate load_source_tier_config(raw), to: Sentinel.Probe.SDK.Config.SourceTier, as: :load
  defdelegate tier_for_handle(config, source_handle), to: Sentinel.Probe.SDK.Config.SourceTier

  # Per-call identifiers.
  defdelegate generate_request_id, to: Sentinel.Probe.SDK.IDs
  defdelegate generate_idempotency_key, to: Sentinel.Probe.SDK.IDs
end
