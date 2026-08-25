defmodule Sentinel.Probe.SDK.ConformanceVectorsTest do
  use ExUnit.Case, async: true

  alias Sentinel.Model.V1.{EventMatch, Int128, ProducerEvent, SpecificationFilter}
  alias Sentinel.Probe.SDK.Int128, as: Int128Codec
  alias Sentinel.Probe.SDK.Internal.SpecMatch

  @root Path.expand("../../../testdata/probe-sdk-conformance", __DIR__)

  defp load(name) do
    document = @root |> Path.join(name) |> File.read!() |> Jason.decode!()
    assert Map.keys(document) |> Enum.sort() == ["cases", "format_version", "kind"]
    assert document["format_version"] == "1.0.0"
    document
  end

  test "manifest suite registry fails closed" do
    manifest = @root |> Path.join("manifest-v1.json") |> File.read!() |> Jason.decode!()
    assert Enum.map(manifest["suites"], & &1["kind"]) == ["spec_match", "int128", "enforcement_gate"]
  end

  test "exact Int128 words and independent decoding follow shared vectors" do
    suite = load("int128-v1.json")
    assert suite["kind"] == "int128"
    assert Enum.uniq_by(suite["cases"], & &1["id"]) == suite["cases"]

    for vector <- suite["cases"] do
      {value, ""} = Integer.parse(vector["value"])
      {high, ""} = Integer.parse(vector["high"])
      {low, ""} = Integer.parse(vector["low"])
      encoded = Int128Codec.from_int(value)
      assert encoded.high == high, vector["id"]
      assert encoded.low == low, vector["id"]
      assert Int128Codec.to_int(%Int128{high: high, low: low}) == value, vector["id"]
    end
  end

  test "SpecMatch follows shared vectors" do
    suite = load("spec-match-v1.json")
    assert suite["kind"] == "spec_match"
    assert Enum.uniq_by(suite["cases"], & &1["id"]) == suite["cases"]

    for vector <- suite["cases"] do
      fixture_match = vector["specification_filter"]["event_match"]

      event_match =
        if fixture_match do
          %EventMatch{
            event_kinds: fixture_match["event_kinds"],
            projected_attribute_keys: fixture_match["projected_attribute_keys"]
          }
        end

      spec = %SpecificationFilter{event_match: event_match}
      event = %ProducerEvent{kind: vector["producer_event"]["kind"]}
      assert SpecMatch.selects?(spec, event) == vector["expected"], vector["id"]
    end
  end
end
