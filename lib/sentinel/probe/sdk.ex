defmodule Sentinel.Probe.SDK do
  @moduledoc """
  Public API entry point for the Sentinel BEAM Probe SDK.

  Delegates to the per-concern modules; a host wires them together. See the
  README for the module map. Trace export and the log receiver are the Probe
  wire: OTLP/HTTP protobuf. The gate reads `DecideResponse` logs and does not
  call Connect.
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

  # OTLP/HTTP export and the loopback log receiver.
  defdelegate export_traces(spans, endpoint, source_handle),
    to: Sentinel.Probe.SDK.Emission.Tracer

  defdelegate export_completeness(signal, endpoint, source_handle),
    to: Sentinel.Probe.SDK.Emission.Tracer

  @doc "Starts the loopback OTLP log receiver. See `Sentinel.Probe.SDK.Otlp.Receiver`."
  @spec start_log_receiver(keyword()) :: GenServer.on_start()
  def start_log_receiver(opts \\ []), do: Sentinel.Probe.SDK.Otlp.Receiver.start_link(opts)
end
