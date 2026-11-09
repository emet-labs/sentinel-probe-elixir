defmodule Sentinel.Probe.SDK.Emission.SpanToEvent do
  @moduledoc """
  Converts an ended span (`Sentinel.Probe.SDK.Emission.Span`) into a
  `ProducerEvent`. Elixir analog of `sdk/go/emission/spantoevent.go`.

  The event's `kind` is the span name, its `occurrence_time` is the span's start
  time, its `attributes` are the span's domain attributes, and its
  `causal_predecessor_ids` come from `sentinel.parent.event.id` and each link's
  `sentinel.event.id`.

  A parent span id or a link span id without that event id is an error. The
  span id is not stored as an Event id or as causality.
  """

  alias Sentinel.Model.V1.{
    AttributeArray,
    AttributeEntry,
    AttributeMap,
    AttributeValue,
    OccurrenceTime,
    ProducerEvent
  }

  alias Sentinel.Probe.SDK.Emission.Span
  alias Sentinel.Probe.SDK.Int128

  @doc """
  Converts a `Span` into a `ProducerEvent`.

  Returns `{:error, reason}` when a predecessor is missing its event id, when
  a `sentinel.*` key is outside the span contract, or when a double is not
  finite. Those spans are not exported.
  """
  @spec span_to_event(Span.t()) :: {:ok, ProducerEvent.t()} | {:error, term()}
  def span_to_event(%Span{} = span) do
    with :ok <- validate_attributes(span.attributes || []),
         {:ok, predecessors} <- collect_causal_predecessors(span) do
      {:ok,
       %ProducerEvent{
         id: span.event_id,
         sequence: span.sequence,
         schema_version: span.schema_version,
         acknowledged_filter_epoch: span.acknowledged_epoch,
         kind: span.name,
         occurrence_time: build_occurrence_time(span.start_time),
         attributes: map_attributes(span.attributes || []),
         claimed_capabilities: span.claimed_capabilities || [],
         claimed_sensitivity: span.claimed_sensitivity || :SENSITIVITY_UNSPECIFIED,
         causal_predecessor_ids: predecessors
       }}
    end
  end

  @doc """
  Converts a span's start time into an `OccurrenceTime` in the "unix" clock
  domain.

  `uncertainty_nanoseconds` is always 0, mirroring `span-to-event.ts:97-103`.

  TODO(#33): `SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY` has no Probe-side input
  today and `Span` deliberately carries no field to supply one, so there is no
  branch to write.
  """
  @spec build_occurrence_time(Span.start_time()) :: OccurrenceTime.t()
  def build_occurrence_time(nil) do
    %OccurrenceTime{clock_domain_id: "unix", nanoseconds: Int128.from_int(0)}
  end

  def build_occurrence_time(start_time) do
    nanos = Int128.time_to_nanoseconds(start_time)

    %OccurrenceTime{
      clock_domain_id: "unix",
      nanoseconds: Int128.from_int(nanos),
      uncertainty_nanoseconds: 0
    }
  end

  @doc "Maps span attributes to `AttributeEntry`s, skipping the reserved contract keys."
  @spec map_attributes([{String.t(), term()}]) :: [AttributeEntry.t()]
  def map_attributes(attributes) do
    attributes
    |> Enum.reject(fn {key, _} -> Span.reserved_attribute?(key) end)
    |> Enum.flat_map(fn {key, value} ->
      case map_value(value) do
        nil -> []
        mapped -> [%AttributeEntry{key: key, value: mapped}]
      end
    end)
  end

  @doc "Maps one tagged attribute value onto the `AttributeValue` oneof."
  @spec map_value(term()) :: AttributeValue.t() | nil
  def map_value({:string, s}), do: %AttributeValue{value: {:string_value, s}}
  def map_value({:bool, b}), do: %AttributeValue{value: {:bool_value, b}}
  def map_value({:int, i}), do: %AttributeValue{value: {:integer_value, i}}
  def map_value({:double, f}), do: %AttributeValue{value: {:double_value, f}}
  def map_value({:bytes, b}), do: %AttributeValue{value: {:bytes_value, b}}

  def map_value({:array, values}) do
    members = values |> Enum.map(&map_value/1) |> Enum.reject(&is_nil/1)
    %AttributeValue{value: {:array_value, %AttributeArray{values: members}}}
  end

  def map_value({:map, entries}) do
    mapped =
      Enum.flat_map(entries, fn {key, value} ->
        case map_value(value) do
          nil -> []
          mapped -> [%AttributeEntry{key: key, value: mapped}]
        end
      end)

    %AttributeValue{value: {:map_value, %AttributeMap{entries: mapped}}}
  end

  def map_value(nil), do: nil

  @doc """
  Collects causal predecessors from the parent event id and every link event
  id, in that order.

  A parent span id or a link without `sentinel.event.id` fails. The span id is
  not returned and is not stored.
  """
  @spec collect_causal_predecessors(Span.t()) :: {:ok, [String.t()]} | {:error, term()}
  def collect_causal_predecessors(%Span{} = span) do
    with {:ok, parent} <- parent_predecessor(span),
         {:ok, links} <- link_predecessors(span) do
      {:ok, parent ++ links}
    end
  end

  defp parent_predecessor(%Span{parent_event_id: eid}) when is_binary(eid) and eid != "",
    do: {:ok, [eid]}

  defp parent_predecessor(%Span{parent_span_id: sid}) when is_binary(sid) and sid != "" do
    {:error, {:missing_parent_event_id, sid}}
  end

  defp parent_predecessor(_), do: {:ok, []}

  defp link_predecessors(%Span{links: links}) do
    (links || [])
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn
      {{eid, _sid}, _index}, {:ok, acc} when is_binary(eid) and eid != "" ->
        {:cont, {:ok, acc ++ [eid]}}

      {{_eid, sid}, index}, _acc ->
        {:halt, {:error, {:missing_link_event_id, index, sid}}}
    end)
  end

  defp validate_attributes(attributes) do
    Enum.reduce_while(attributes, :ok, fn {key, value}, :ok ->
      cond do
        Span.sentinel_attribute?(key) and not Span.reserved_attribute?(key) ->
          {:halt, {:error, {:unknown_sentinel_attribute, key}}}

        true ->
          case validate_value(key, value) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
      end
    end)
  end

  defp validate_value(_key, nil), do: :ok
  defp validate_value(key, {:double, f}), do: finite_double(key, f)

  defp validate_value(key, {:array, values}) when is_list(values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case validate_value(key, value) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_value(_key, {:map, entries}) when is_list(entries) do
    Enum.reduce_while(entries, :ok, fn {inner, value}, :ok ->
      if Span.sentinel_attribute?(inner) do
        {:halt, {:error, {:unknown_sentinel_attribute, inner}}}
      else
        case validate_value(inner, value) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    end)
  end

  defp validate_value(key, {:int, n}) when is_integer(n) do
    if n >= -9_223_372_036_854_775_808 and n <= 9_223_372_036_854_775_807 do
      :ok
    else
      {:error, {:int_out_of_range, key}}
    end
  end

  defp validate_value(_, _), do: :ok

  defp finite_double(key, f) when is_float(f) do
    if match?(<<_::1, 2047::11, _::52>>, <<f::float-64>>) do
      {:error, {:non_finite_double, key}}
    else
      :ok
    end
  end

  defp finite_double(key, _), do: {:error, {:non_finite_double, key}}
end
