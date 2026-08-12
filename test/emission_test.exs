defmodule Sentinel.Probe.SDK.EmissionTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{
    AttributeArray,
    AttributeEntry,
    AttributeMap,
    AttributeValue,
    ProducerEvent
  }

  alias Sentinel.Probe.SDK.Emission.{Span, SpanToEvent}

  defp span(opts \\ []) do
    attrs = Keyword.get(opts, :attributes, [])

    %Span{
      event_id: Keyword.get(opts, :event_id, "evt-1"),
      schema_version: "sentinel.model.v1",
      name: Keyword.get(opts, :name, "span.name"),
      start_time: Keyword.get(opts, :start_time, {1_700_000_000, 123_456_789}),
      attributes: attrs,
      links: Keyword.get(opts, :links),
      parent_event_id: Keyword.get(opts, :parent_event_id),
      parent_span_id: Keyword.get(opts, :parent_span_id),
      on_malformed_link: Keyword.get(opts, :on_malformed_link)
    }
  end

  test "span_to_event maps the span name to kind" do
    event = SpanToEvent.span_to_event(span(name: "http.request"))
    assert event.kind == "http.request"
  end

  test "span_to_event populates occurrence_time from start_time" do
    event = SpanToEvent.span_to_event(span(start_time: {1_700_000_000, 1}))
    ot = event.occurrence_time
    assert ot.clock_domain_id == "unix"
    assert ot.uncertainty_nanoseconds == 0
  end

  test "occurrence_time nanoseconds are exact" do
    event = SpanToEvent.span_to_event(span(start_time: {1_700_000_000, 1}))
    assert event.occurrence_time.nanoseconds.high == 0
    assert event.occurrence_time.nanoseconds.low == 1_700_000_000_000_000_001
  end

  test "string attribute" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:string, "v"}}]))
    assert [entry] = event.attributes
    assert entry.key == "k"
    assert entry.value.value == {:string_value, "v"}
  end

  test "bool attribute" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:bool, true}}]))
    assert hd(event.attributes).value.value == {:bool_value, true}
  end

  test "int attribute (sint64)" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:int, 42}}]))
    assert hd(event.attributes).value.value == {:integer_value, 42}
  end

  test "double attribute" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:double, 3.14}}]))
    assert hd(event.attributes).value.value == {:double_value, 3.14}
  end

  test "bytes attribute" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:bytes, <<1, 2, 3>>}}]))
    assert hd(event.attributes).value.value == {:bytes_value, <<1, 2, 3>>}
  end

  test "array attribute" do
    event =
      SpanToEvent.span_to_event(span(attributes: [{"k", {:array, [{:string, "a"}, {:int, 1}]}}]))

    {:array_value, %AttributeArray{values: members}} = hd(event.attributes).value.value
    assert length(members) == 2
    assert hd(members).value == {:string_value, "a"}
  end

  test "map attribute" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:map, [{"inner", {:bool, true}}]}}]))
    {:map_value, %AttributeMap{entries: entries}} = hd(event.attributes).value.value
    assert [entry] = entries
    assert entry.key == "inner"
    assert entry.value.value == {:bool_value, true}
  end

  test "nil attribute members are skipped" do
    event = SpanToEvent.span_to_event(span(attributes: [{"k", {:array, [nil, {:string, "x"}]}}]))
    {:array_value, %AttributeArray{values: members}} = hd(event.attributes).value.value
    assert length(members) == 1
  end

  test "reserved keys are stripped from attributes" do
    event =
      SpanToEvent.span_to_event(
        span(
          attributes: [
            {Span.attribute_event_id(), {:string, "evt-id"}},
            {Span.attribute_parent_event_id(), {:string, "parent-id"}},
            {"normal", {:string, "v"}}
          ]
        )
      )

    keys = Enum.map(event.attributes, & &1.key)
    assert "normal" in keys
    refute Span.attribute_event_id() in keys
    refute Span.attribute_parent_event_id() in keys
  end

  test "causal predecessor from parent_event_id" do
    event = SpanToEvent.span_to_event(span(parent_event_id: "parent-evt"))
    assert event.causal_predecessor_ids == ["parent-evt"]
  end

  test "causal predecessor falls back to parent_span_id" do
    event =
      SpanToEvent.span_to_event(
        span(parent_span_id: "abc123", on_malformed_link: fn _, _ -> :ok end)
      )

    assert "abc123" in event.causal_predecessor_ids
  end

  test "causal predecessor from links with event_id" do
    event = SpanToEvent.span_to_event(span(links: [{"link-evt", "span1"}]))
    assert "link-evt" in event.causal_predecessor_ids
  end

  test "causal predecessor falls back to link span_id" do
    event = SpanToEvent.span_to_event(span(links: [{nil, "link-span"}]))
    assert "link-span" in event.causal_predecessor_ids
  end

  test "event_id is set from the reserved key" do
    event = SpanToEvent.span_to_event(span(event_id: "my-evt"))
    assert event.id == "my-evt"
  end
end
