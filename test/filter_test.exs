defmodule Sentinel.Probe.SDK.FilterTest do
  use ExUnit.Case, async: true

  import Sentinel.Probe.SDK.TestHelpers

  alias Sentinel.Model.V1.{EventFilter, EventMatch, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.SDK.Filter

  defp attr(key), do: string_attr(key, "v-#{key}")

  test "a nil event is dropped" do
    assert Filter.apply_filter(nil, make_filter(5, [ask_and_block_spec()])) == nil
  end

  test "a nil filter drops (no specifications to select)" do
    assert Filter.apply_filter(make_event(test_kind()), nil) == nil
  end

  test "an event no spec selects is dropped" do
    spec = make_spec("s", ["other.kind"], :FAIL_MODE_OPEN, :DELIVERY_MODE_ASK_AND_BLOCK)
    assert Filter.apply_filter(make_event(test_kind()), make_filter(5, [spec])) == nil
  end

  test "a selecting spec with empty projected_attribute_keys keeps every attribute" do
    event = make_event(test_kind(), [attr("a"), attr("b"), attr("c")])

    spec = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: []}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec]))
    assert out.kind == test_kind()
    assert Enum.map(out.attributes, & &1.key) == ~w(a b c)
  end

  test "the union of projected keys is kept, others trimmed" do
    event = make_event(test_kind(), [attr("a"), attr("b"), attr("c"), attr("d")])

    spec1 = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: ["a", "b"]}
    }

    spec2 = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: ["b", "c"]}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec1, spec2]))
    assert Enum.map(out.attributes, & &1.key) == ~w(a b c)
  end

  test "causal_predecessor_ids are never trimmed" do
    event = %ProducerEvent{
      id: "evt-1",
      kind: test_kind(),
      schema_version: "sentinel.model.v1",
      attributes: [attr("a"), attr("b")],
      causal_predecessor_ids: ["parent-1", "parent-2"]
    }

    spec = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: ["a"]}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec]))
    assert out.causal_predecessor_ids == ["parent-1", "parent-2"]
    assert Enum.map(out.attributes, & &1.key) == ~w(a)
  end

  test "every other field is carried through unchanged" do
    seq = sequence(7, 99)

    event = %ProducerEvent{
      id: "evt-1",
      sequence: seq,
      schema_version: "1.2.3",
      acknowledged_filter_epoch: 5,
      kind: test_kind(),
      occurrence_time: nil,
      attributes: [attr("a")],
      claimed_capabilities: [:SOURCE_CAPABILITY_CAUSAL_EDGES],
      claimed_sensitivity: :SENSITIVITY_CONFIDENTIAL,
      causal_predecessor_ids: ["p"]
    }

    spec = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: ["a"]}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec]))
    assert out.id == "evt-1"
    assert out.sequence == seq
    assert out.schema_version == "1.2.3"
    assert out.acknowledged_filter_epoch == 5
    assert out.claimed_capabilities == [:SOURCE_CAPABILITY_CAUSAL_EDGES]
    assert out.claimed_sensitivity == :SENSITIVITY_CONFIDENTIAL
  end

  test "a spec with nil EventMatch projects all attributes (over-approximate)" do
    event = make_event(test_kind(), [attr("a"), attr("b")])
    spec = %SpecificationFilter{event_match: nil}
    out = Filter.apply_filter(event, make_filter(5, [spec]))
    assert Enum.map(out.attributes, & &1.key) == ~w(a b)
  end

  test "the keep-everything branch returns a copy of the attribute list" do
    event = make_event(test_kind(), [attr("a")])

    spec = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: []}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec]))
    # Immutable data makes aliasing safe in Elixir, but the list is still a fresh
    # allocation so a caller cannot observe the input list through it.
    assert Enum.map(out.attributes, & &1.key) == ~w(a)
  end

  test "any selecting spec with empty keys wins over union specs (keep all)" do
    event = make_event(test_kind(), [attr("a"), attr("b"), attr("c")])

    spec1 = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: ["a"]}
    }

    spec2 = %SpecificationFilter{
      event_match: %EventMatch{event_kinds: [test_kind()], projected_attribute_keys: []}
    }

    out = Filter.apply_filter(event, make_filter(5, [spec1, spec2]))
    assert Enum.map(out.attributes, & &1.key) == ~w(a b c)
  end
end
