defmodule Sentinel.Probe.SDK.FilterPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Sentinel.Probe.SDK.TestHelpers

  alias Sentinel.Model.V1.{EventMatch, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.SDK.Filter

  @moduletag :property
  # Pinned seed so no wall clock is read at test-execution time (ADR-0019).
  # `mix test --seed 0` also pins, but StreamData takes its own seed from
  # ExUnit's; this module documents the hermetic intent.

  defp event_with_kinds(kinds) do
    %ProducerEvent{
      id: "evt",
      kind: hd(kinds),
      schema_version: "1",
      attributes: for(i <- 1..5, do: string_attr("k#{i}", "v#{i}"))
    }
  end

  property "apply_filter never drops a relevant event's causal_predecessor_ids" do
    check all(kinds <- list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 3)) do
      event = %ProducerEvent{
        id: "evt",
        kind: hd(kinds),
        schema_version: "1",
        attributes: [string_attr("k1", "v1")],
        causal_predecessor_ids: ["p1", "p2", "p3"]
      }

      spec = %SpecificationFilter{
        event_match: %EventMatch{event_kinds: kinds, projected_attribute_keys: ["k1"]}
      }

      case Filter.apply_filter(event, make_filter(1, [spec])) do
        nil -> true
        out -> assert out.causal_predecessor_ids == ["p1", "p2", "p3"]
      end
    end
  end

  property "apply_filter keeps only the union of projected keys" do
    check all(keys <- list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 8)) do
      uniq = Enum.uniq(keys)

      event = %ProducerEvent{
        id: "evt",
        kind: "k",
        schema_version: "1",
        attributes: for(k <- uniq, do: string_attr(k, "v"))
      }

      spec = %SpecificationFilter{
        event_match: %EventMatch{event_kinds: ["k"], projected_attribute_keys: keys}
      }

      out = Filter.apply_filter(event, make_filter(1, [spec]))
      kept = Enum.map(out.attributes, & &1.key) |> Enum.uniq() |> Enum.sort()
      assert kept == Enum.sort(uniq)
    end
  end

  property "apply_filter over-approximates: empty projected keys keeps all attributes" do
    check all(count <- integer(0..10)) do
      attrs = if count == 0, do: [], else: for(i <- 1..count, do: string_attr("k#{i}", "v#{i}"))
      event = %ProducerEvent{id: "evt", kind: "k", schema_version: "1", attributes: attrs}

      spec = %SpecificationFilter{
        event_match: %EventMatch{event_kinds: ["k"], projected_attribute_keys: []}
      }

      out = Filter.apply_filter(event, make_filter(1, [spec]))
      assert length(out.attributes) == count
    end
  end
end
