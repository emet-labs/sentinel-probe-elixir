defmodule Sentinel.Probe.SDK.Otlp.PartialSuccess do
  @moduledoc """
  Reads the `ExportTraceServiceResponse` Sentinel returns for a trace export.

  An export succeeded only on HTTP 2xx with `partial_success.rejected_spans ==
  0`; an absent `partial_success` counts as 0. Sentinel counts conflicts,
  admission refusals and Events above a closed boundary as rejected, so a
  rejected span is an Event Sentinel does not hold. A duplicate is reported with
  zero rejected spans and a message. A body that does not decode is a failure,
  never a success.

  The fields are decoded by hand (`ExportTraceServiceResponse.partial_success
  = 1`, `ExportTracePartialSuccess.rejected_spans = 1`, an int64, and
  `error_message = 2`, a string); other fields are skipped by wire type.
  """

  import Bitwise

  @int64_max 9_223_372_036_854_775_807

  @doc "Returns `{:ok, rejected_spans}` for a 2xx body, or `{:error, reason}`."
  @spec rejected_spans(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def rejected_spans(body) when is_binary(body) do
    with {:ok, rejected, _message} <- rejection(body), do: {:ok, rejected}
  end

  @doc """
  Returns `{:ok, rejected_spans, error_message}` for a 2xx body (the message is
  `""` when there is none), or `{:error, reason}`.
  """
  @spec rejection(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def rejection(body) when is_binary(body) do
    with {:ok, rejected, message} <- response(body, 0, "") do
      if rejected > @int64_max,
        do: {:error, {:negative_rejected_spans, rejected - (1 <<< 64)}},
        else: {:ok, rejected, message}
    end
  end

  @doc """
  True when `message` names a refused duplicate Event id: Sentinel answered
  `evidence_conflict` or `identity_conflict`, so it already holds an Event under
  that id.
  """
  @spec duplicate_event_id?(binary()) :: boolean()
  def duplicate_event_id?(message) when is_binary(message),
    do: String.contains?(message, ["evidence_conflict", "identity_conflict"])

  # ExportTraceServiceResponse: repeated occurrences of field 1 merge, so the
  # last rejected_spans and error_message seen win.
  defp response(<<>>, rejected, text), do: {:ok, rejected, text}

  defp response(bytes, rejected, text) do
    with {:ok, field, wire_type, rest} <- tag(bytes) do
      case {field, wire_type} do
        {1, 2} ->
          with {:ok, message, rest} <- length_delimited(rest),
               {:ok, rejected, text} <- partial(message, rejected, text),
               do: response(rest, rejected, text)

        _ ->
          with {:ok, rest} <- skip(wire_type, rest), do: response(rest, rejected, text)
      end
    end
  end

  # ExportTracePartialSuccess.
  defp partial(<<>>, rejected, text), do: {:ok, rejected, text}

  defp partial(bytes, rejected, text) do
    with {:ok, field, wire_type, rest} <- tag(bytes) do
      case {field, wire_type} do
        {1, 0} ->
          with {:ok, value, rest} <- varint(rest), do: partial(rest, value, text)

        {2, 2} ->
          with {:ok, value, rest} <- length_delimited(rest), do: partial(rest, rejected, value)

        _ ->
          with {:ok, rest} <- skip(wire_type, rest), do: partial(rest, rejected, text)
      end
    end
  end

  defp tag(bytes) do
    with {:ok, key, rest} <- varint(bytes) do
      if key >>> 3 == 0,
        do: {:error, :invalid_field_number},
        else: {:ok, key >>> 3, key &&& 7, rest}
    end
  end

  defp skip(0, bytes), do: with({:ok, _, rest} <- varint(bytes), do: {:ok, rest})
  defp skip(1, <<_::binary-size(8), rest::binary>>), do: {:ok, rest}
  defp skip(2, bytes), do: with({:ok, _, rest} <- length_delimited(bytes), do: {:ok, rest})
  defp skip(5, <<_::binary-size(4), rest::binary>>), do: {:ok, rest}
  defp skip(wire_type, _bytes) when wire_type in [1, 5], do: {:error, :truncated}
  defp skip(wire_type, _bytes), do: {:error, {:unsupported_wire_type, wire_type}}

  defp length_delimited(bytes) do
    with {:ok, size, rest} <- varint(bytes) do
      case rest do
        <<value::binary-size(size), rest::binary>> -> {:ok, value, rest}
        _ -> {:error, :truncated}
      end
    end
  end

  # A base-128 varint of at most ten bytes, as an unsigned 64-bit value.
  defp varint(bytes), do: varint(bytes, 0, 0)

  defp varint(_bytes, _value, 10), do: {:error, :varint_too_long}
  defp varint(<<>>, _value, _index), do: {:error, :truncated}

  defp varint(<<1::1, low::7, rest::binary>>, value, index),
    do: varint(rest, value ||| low <<< (7 * index), index + 1)

  defp varint(<<0::1, low::7, rest::binary>>, value, index) do
    value = value ||| low <<< (7 * index)

    if value > 0xFFFF_FFFF_FFFF_FFFF,
      do: {:error, :varint_overflow},
      else: {:ok, value, rest}
  end
end
