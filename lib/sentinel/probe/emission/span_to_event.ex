defmodule Sentinel.Probe.SDK.Emission.SpanToEvent do
  @moduledoc """
  Converts an ended span (`Sentinel.Probe.SDK.Emission.Span`) into a
  `ProducerEvent`. Elixir analog of `sdk/go/emission/spantoevent.go`.

  The event's `kind` is the span name, its `occurrence_time` is the span's start
  time, its `attributes` are the span's attributes minus the two reserved keys,
  and its `causal_predecessor_ids` come from the parent and link edges described
  in the package doc.
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

  @doc "Converts a `Span` into a `ProducerEvent`."
  @spec span_to_event(Span.t()) :: ProducerEvent.t()
  def span_to_event(%Span{} = span) do
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
      causal_predecessor_ids: collect_causal_predecessors(span)
    }
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

  @doc "Maps span attributes to `AttributeEntry`s, skipping the two reserved keys."
  @spec map_attributes([{String.t(), term()}]) :: [AttributeEntry.t()]
  def map_attributes(attributes) do
    attributes
    |> Enum.reject(fn {key, _} ->
      key == Span.attribute_event_id() or key == Span.attribute_parent_event_id()
    end)
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
  Collects the causal predecessors from the parent edge and every link edge, in
  that order.

  The parent carries the parent's event ID under the reserved key when present;
  otherwise the parent's span ID is used and `on_malformed_link` is invoked with
  `\"parent\"`. Each link carries its target's event ID in that link's OWN
  attributes when present; otherwise the link's span ID is used and
  `on_malformed_link` is invoked with `\"link\"`.
  """
  @spec collect_causal_predecessors(Span.t()) :: [String.t()]
  def collect_causal_predecessors(%Span{} = span) do
    parent = parent_predecessor(span) ++ link_predecessors(span)
    parent
  end

  defp parent_predecessor(%Span{parent_event_id: eid}) when eid != nil, do: [eid]

  defp parent_predecessor(%Span{parent_span_id: sid, on_malformed_link: cb}) when sid != nil do
    malformed(cb, Span.malformed_link_source_parent(), sid)
    [sid]
  end

  defp parent_predecessor(_), do: []

  defp link_predecessors(%Span{links: links, on_malformed_link: cb}) do
    Enum.flat_map(links || [], fn
      {eid, _sid} when eid != nil ->
        [eid]

      {nil, sid} ->
        malformed(cb, Span.malformed_link_source_link(), sid)
        [sid]
    end)
  end

  defp malformed(nil, _source, _sid), do: :ok
  defp malformed(cb, source, sid), do: cb.(source, sid)
end
