defmodule Sentinel.Probe.SDK.SpecMatchTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventMatch, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  defp event(kind), do: %ProducerEvent{kind: kind}

  test "an empty event_kinds list selects everything" do
    spec = %SpecificationFilter{event_match: %EventMatch{event_kinds: []}}
    assert SpecMatch.selects?(spec, event("anything"))
  end

  test "a nil EventMatch selects everything (over-approximate upward)" do
    spec = %SpecificationFilter{event_match: nil}
    assert SpecMatch.selects?(spec, event("anything"))
  end

  test "a nil spec selects everything" do
    assert SpecMatch.selects?(spec(nil), event("anything"))
  end

  defp spec(nil), do: nil

  test "matching kind selects" do
    spec = %SpecificationFilter{event_match: %EventMatch{event_kinds: ["a", "b"]}}
    assert SpecMatch.selects?(spec, event("a"))
    assert SpecMatch.selects?(spec, event("b"))
  end

  test "non-matching kind does not select" do
    spec = %SpecificationFilter{event_match: %EventMatch{event_kinds: ["a"]}}
    refute SpecMatch.selects?(spec, event("c"))
  end

  test "an event with an empty kind is selected only by an empty-kinds spec" do
    spec = %SpecificationFilter{event_match: %EventMatch{event_kinds: ["a"]}}
    refute SpecMatch.selects?(spec, event(""))
    empty = %SpecificationFilter{event_match: %EventMatch{event_kinds: []}}
    assert SpecMatch.selects?(empty, event(""))
  end
end
