defmodule Sentinel.Probe.SDK.Otlp.Exporter do
  @moduledoc """
  Sends OTLP/HTTP protobuf to Sentinel.

  Traces go to `/v1/traces` and producer completeness goes to `/v1/logs`,
  both with `Content-Type: application/x-protobuf` and
  `x-sentinel-source-handle`. This path does not call Connect.
  """

  alias Sentinel.Model.V1.ProducerCompletenessSignal
  alias Sentinel.Probe.SDK.Emission.Span
  alias Sentinel.Probe.SDK.Otlp.{Codec, Http}

  @handle "x-sentinel-source-handle"

  @doc """
  Exports ended spans. A missing predecessor id fails the export.

  `opts[:resource_attributes]` is a list of `{key, value}` OTLP Resource
  attributes (for example `[{"service.name", "shipping"}]`) sent on the
  request's Resource. A non-2xx response is `{:error, {:http_status, status,
  body}}` with the response body; `Sentinel.Probe.SDK.Otlp.Http.format_error/1`
  renders it for logs.
  """
  @spec export_traces(String.t(), String.t(), [Span.t()], keyword()) ::
          {:ok, binary()} | {:error, term()}
  def export_traces(endpoint, source_handle, spans, opts \\ [])
      when is_binary(endpoint) and is_binary(source_handle) and is_list(spans) do
    with {:ok, body} <- Codec.encode_traces(spans, opts) do
      Http.post(endpoint, "/v1/traces", [{@handle, source_handle}], body)
    end
  end

  @doc "Exports one producer completeness signal as an OTLP log."
  @spec export_completeness(String.t(), String.t(), ProducerCompletenessSignal.t()) ::
          {:ok, binary()} | {:error, term()}
  def export_completeness(endpoint, source_handle, %ProducerCompletenessSignal{} = signal)
      when is_binary(endpoint) and is_binary(source_handle) do
    with {:ok, body} <- Codec.encode_completeness(signal, source_handle) do
      Http.post(endpoint, "/v1/logs", [{@handle, source_handle}], body)
    end
  end
end
