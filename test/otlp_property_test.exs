defmodule Sentinel.Probe.SDK.OtlpPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Sentinel.Model.V1.SequenceCoordinate
  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}
  alias Sentinel.Probe.SDK.Int128
  alias Sentinel.Probe.SDK.Otlp.Codec

  @moduletag :property

  property "valid spans round-trip to the same producer event" do
    check all(span <- valid_span()) do
      assert {:ok, event} = SpanToEvent.span_to_event(span)
      assert {:ok, bytes} = Codec.encode_traces([span])
      assert {:ok, [decoded]} = Codec.decode_traces(bytes)

      assert decoded.id == event.id
      assert decoded.kind == event.kind
      assert decoded.schema_version == event.schema_version
      assert decoded.sequence == event.sequence
      assert decoded.acknowledged_filter_epoch == event.acknowledged_filter_epoch
      assert decoded.claimed_sensitivity == event.claimed_sensitivity
      assert decoded.claimed_capabilities == event.claimed_capabilities
      assert decoded.causal_predecessor_ids == event.causal_predecessor_ids
      assert decoded.occurrence_time.clock_domain_id == "unix"
      assert decoded.occurrence_time.uncertainty_nanoseconds == 0

      assert Int128.to_int(decoded.occurrence_time.nanoseconds) ==
               Int128.time_to_nanoseconds(span.start_time)

      assert attribute_pairs(decoded) == attribute_pairs(event)
      refute Enum.any?(decoded.causal_predecessor_ids, &(&1 == "parent-span-id"))
    end
  end

  property "a missing predecessor id is not stored as an event id" do
    check all(
            span_id <- string(:alphanumeric, min_length: 1, max_length: 24),
            link? <- boolean()
          ) do
      span =
        if link? do
          base_span(%{links: [{nil, span_id}]})
        else
          base_span(%{parent_span_id: span_id})
        end

      assert {:error, reason} = SpanToEvent.span_to_event(span)
      assert {:error, ^reason} = Codec.encode_traces([span])
      assert reason_mentions_span_id?(reason, span_id)
      refute match?({:ok, _}, SpanToEvent.span_to_event(span))
    end
  end

  property "arbitrary log bytes do not raise" do
    check all(bytes <- binary(max_length: 48)) do
      assert match?({:ok, _}, Codec.decode_probe_logs(bytes)) or
               match?({:error, _}, Codec.decode_probe_logs(bytes))
    end
  end

  defp valid_span do
    gen all(
          name <- string(:alphanumeric, min_length: 1, max_length: 12),
          event_id <- string(:alphanumeric, min_length: 1, max_length: 16),
          parent <- one_of([constant(nil), string(:alphanumeric, min_length: 1, max_length: 12)]),
          link <- one_of([constant(nil), string(:alphanumeric, min_length: 1, max_length: 12)]),
          attrs <- list_of(attr_pair(), max_length: 3),
          epoch <- integer(0..30),
          number <- integer(0..30),
          filter_epoch <- integer(0..30),
          sensitivity <-
            member_of([
              :SENSITIVITY_PUBLIC,
              :SENSITIVITY_INTERNAL,
              :SENSITIVITY_CONFIDENTIAL,
              :SENSITIVITY_RESTRICTED
            ]),
          start_sec <- integer(1..1_800_000_000)
        ) do
      links = if link, do: [{link, "link-span"}], else: []

      %Span{
        event_id: "e" <> event_id,
        name: "k" <> name,
        sequence: %SequenceCoordinate{epoch: epoch, sequence: number},
        schema_version: "sagashop-contract-v1",
        acknowledged_epoch: filter_epoch,
        claimed_sensitivity: sensitivity,
        claimed_capabilities: [],
        start_time: {start_sec, 0},
        attributes: Enum.uniq_by(attrs, fn {key, _} -> key end),
        parent_event_id: parent,
        parent_span_id: if(parent, do: "parent-span-id"),
        links: links
      }
    end
  end

  defp base_span(overrides) do
    Map.merge(
      %Span{
        event_id: "evt",
        name: "kind",
        sequence: %SequenceCoordinate{epoch: 7, sequence: 1},
        schema_version: "sagashop-contract-v1",
        acknowledged_epoch: 1,
        claimed_sensitivity: :SENSITIVITY_INTERNAL,
        start_time: {1_700_000_000, 0},
        attributes: [{"sagashop.order.id", {:string, "order-1"}}]
      },
      overrides
    )
  end

  defp attr_pair do
    gen all(
          key <- string(:alphanumeric, min_length: 1, max_length: 6),
          value <-
            one_of([
              map(string(:alphanumeric, max_length: 8), &{:string, &1}),
              map(boolean(), &{:bool, &1}),
              map(integer(-1000..1000), &{:int, &1}),
              map(float(min: -50.0, max: 50.0), &{:double, &1})
            ])
        ) do
      {"a" <> key, value}
    end
  end

  defp attribute_pairs(event) do
    Enum.map(event.attributes, fn entry -> {entry.key, entry.value.value} end)
  end

  defp reason_mentions_span_id?({:missing_parent_event_id, span_id}, span_id), do: true
  defp reason_mentions_span_id?({:missing_link_event_id, _index, span_id}, span_id), do: true
  defp reason_mentions_span_id?(_, _), do: false
end
